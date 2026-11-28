using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Stream;

/// <summary>
/// The default <see cref="IStreamClient"/>: stream append sessions, reads, and commit subscriptions.
/// </summary>
/// <remarks>
/// Obtained from <see cref="FitzClient"/> rather than constructed directly. The public
/// constructors exist for testing against a transport delegate.
/// </remarks>
sealed class StreamClient : IStreamClient, IAsyncDisposable
{
    const int MaxConcurrentSubscriptionChanges = 16;

    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _retryRequest;
    readonly Func<Action, IDisposable>? _registerOnDisconnect;
    readonly FitzConnection? _connection;
    readonly TimeProvider _time;
    readonly SubscriptionTable<StreamCommitEvent> _subscriptions;
    readonly IDisposable? _connectionBinding;
    bool _disposed;

    internal StreamClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterNotificationHandler,
            connection.OnDisconnect,
            (handler, rejected) => connection.TryDispatchAsyncHandler("stream", handler, rejected),
            connection.RequestWithRetryAsync,
            connection.SubscriptionBufferCapacity,
            connection.Time)
    {
        _connection = connection;
        _connectionBinding = _subscriptions.AttachTo(connection);
    }

    /// <summary>
    /// Creates a domain client over a request delegate, for testing without a broker.
    /// </summary>
    public StreamClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null
    )
        : this(
            async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)),
            NotificationRegistrationAdapter.Adapt(registerNotificationHandler))
    {
        ArgumentNullException.ThrowIfNull(request);
    }

    internal StreamClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null,
        Func<Action, IDisposable>? registerOnDisconnect = null,
        AsyncHandlerDispatch? dispatchAsyncHandler = null,
        Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? retryRequest = null,
        int subscriptionBufferCapacity = SubscriptionRegistration<StreamCommitEvent>.DefaultCapacity,
        TimeProvider? timeProvider = null
    )
    {
        _request = request;
        _time = timeProvider ?? TimeProvider.System;
        _registerOnDisconnect = registerOnDisconnect;
        _retryRequest = retryRequest;
        _subscriptions = new SubscriptionTable<StreamCommitEvent>(
            this,
            "stream",
            MessageTypes.StreamNotify,
            registerNotificationHandler,
            dispatchAsyncHandler,
            subscriptionBufferCapacity,
            SubscribeWireAsync,
            UnsubscribeWireAsync,
            DecodeNotification,
            _time,
            maxConcurrentChanges: MaxConcurrentSubscriptionChanges);
    }

    /// <inheritdoc />
    public async Task DropAsync(string selector, StreamCommitMode mode, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ScopedDrop.Validate(selector, "stream");

        if (mode is not StreamCommitMode.Buffered and not StreamCommitMode.Sync)
            throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown persistence policy");

        using var writer = new BinaryBufferWriter();
        writer.WriteString(selector);
        writer.WriteU8((byte)mode);

        var response = await _request(MessageTypes.StreamDrop, writer.WrittenMemory, ct).ConfigureAwait(false);
        StreamWireHelpers.EnsureSuccessStatusOnly(response, "DROP");
    }

    /// <inheritdoc />
    public async Task<IStreamSession> BeginAsync(string route, ReadOnlyMemory<byte>? ingestMetadata = null, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ValidateExactStreamRoute(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        if (ingestMetadata.HasValue && ingestMetadata.Value.Length > 0)
        {
            writer.WriteU8(1);
            writer.WriteLengthPrefixed(ingestMetadata.Value.Span);
        }
        else
        {
            writer.WriteU8(0);
        }


        using var session = new SessionRequestContext(_connection, _request, _registerOnDisconnect);
        var response = await session.RequestAsync(MessageTypes.StreamBegin, writer.WrittenMemory, ct).ConfigureAwait(false);
        var sessionId = StreamWireHelpers.ReadBeginSessionId(response);
        session.EnsureValid();
        return new StreamSession(session.BoundRequest, sessionId, session.RegisterOnDisconnect, _time);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<StreamReadBatch> ReadAsync(
        string route,
        ulong fromOffset,
        StreamReadMode mode,
        StreamReadOptions? options = null,
        CancellationToken ct = default
    )
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(route);

        if (!RouteValidation.TryGetStreamSelectorScope(route, out var scope))
            throw new ArgumentException($"Invalid stream selector '{route}'.", nameof(route));


        if (mode is not (StreamReadMode.Replay or StreamReadMode.Follow))
            throw new ArgumentOutOfRangeException(nameof(mode));

        options ??= new StreamReadOptions();
        return mode == StreamReadMode.Replay
            ? ReadSnapshotAsync(route, scope, fromOffset, options, default, ct)
            : FollowAsync(route, scope, fromOffset, options, ct);
    }

    async IAsyncEnumerable<StreamReadBatch> FollowAsync(
        string route,
        StreamSelectorScope scope,
        ulong fromOffset,
        StreamReadOptions options,
        [EnumeratorCancellation] CancellationToken ct
    )
    {
        // One pending wake covers any number of commits during a read or while yielding a batch.
        var wake = Channel.CreateBounded<bool>(1);
        var binding = await _subscriptions.SubscribeAsync(route, (_, _) =>
        {
            wake.Writer.TryWrite(true);
            return ValueTask.CompletedTask;
        }, preDispatch: null, ct, bestEffort: true).ConfigureAwait(false);
        IAsyncDisposable subscription = new StreamSubscription(route, binding.Unsubscribe, binding.Completion, _time);
        await using var cleanup = subscription.ConfigureAwait(false);
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(ct, binding.Ended);

        try
        {
            while (true)
            {
                lifetime.Token.ThrowIfCancellationRequested();
                var session = _connection is null ? CancellationToken.None
                    : await _connection.WaitUntilReadyAsync(Timeout.InfiniteTimeSpan, lifetime.Token).ConfigureAwait(false);
                using var snapshot = CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token, session);

                await foreach (var batch in ReadSnapshotAsync(route, scope, fromOffset, options, session, snapshot.Token).ConfigureAwait(false))
                {
                    lifetime.Token.ThrowIfCancellationRequested();
                    yield return batch;
                    fromOffset = batch.NextOffset;
                }


                try
                {
                    _ = await wake.Reader.ReadAsync(snapshot.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (session.IsCancellationRequested && !lifetime.IsCancellationRequested)
                {
                    // Restart with a fresh snapshot after the replacement session restores subscriptions.
                }
            }
        }
        finally
        {
            // Preserve a delivery failure that interrupted a read or wait.
            if (binding.Ended.IsCancellationRequested && !ct.IsCancellationRequested)
                await binding.Completion.ConfigureAwait(false);
        }
    }

    async IAsyncEnumerable<StreamReadBatch> ReadSnapshotAsync(
        string route,
        StreamSelectorScope scope,
        ulong fromOffset,
        StreamReadOptions options,
        CancellationToken session,
        [EnumeratorCancellation] CancellationToken ct
    )
    {
        ulong? fingerprint = null;
        ulong? watermark = null;
        ulong? previousNext = null;

        while (!session.IsCancellationRequested)
        {
            StreamReadPage page;

            try
            {
                ct.ThrowIfCancellationRequested();
                page = await ReadPageAsync(route, fromOffset, options, fingerprint, watermark, session, ct).ConfigureAwait(false);
            }
            catch (Exception error) when (session.IsCancellationRequested &&
                error is OperationCanceledException or ConnectionException or IOException)
            {
                yield break;
            }


            var next = page.NextOffset(scope, fromOffset);

            if (page.Cursor.HasMore)
            {
                if (next == previousNext)
                    throw new StreamException("Two consecutive READ pages have a stalled cursor.", "READ_STALLED");

                previousNext = next;

                if (scope == StreamSelectorScope.Global)
                {
                    fingerprint = page.Cursor.CursorFingerprint
                        ?? throw new StreamException("READ response is missing the global cursor fingerprint", "READ_INVALID_RESPONSE");
                    watermark = page.Cursor.CapturedWatermark
                        ?? throw new StreamException("READ response is missing the captured watermark", "READ_INVALID_RESPONSE");
                }
            }

            yield return new StreamReadBatch(page.Items, fromOffset, next, !page.Cursor.HasMore);

            if (!page.Cursor.HasMore)
                yield break;

            fromOffset = next;
        }
    }

    async Task<StreamReadPage> ReadPageAsync(
        string route,
        ulong startOffset,
        StreamReadOptions options,
        ulong? cursorFingerprint,
        ulong? capturedWatermark,
        CancellationToken session,
        CancellationToken ct
    )
    {
        ThrowIfDisposed();
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU64(startOffset);
        writer.WriteU64(options.BatchSize);
        var maxBytes = options.MaxBytes;
        writer.WriteU8((byte)(maxBytes.HasValue ? 1 : 0));

        if (maxBytes.HasValue)
        {
            writer.WriteU64(maxBytes.Value);
        }


        var filterBytes = StreamWireHelpers.EncodeStreamFilterSet(options.Filter);
        writer.WriteU8((byte)(filterBytes.Length > 0 ? 1 : 0));

        if (filterBytes.Length > 0)
        {
            writer.WriteLengthPrefixed(filterBytes);
        }


        writer.WriteU8((byte)(cursorFingerprint.HasValue ? 1 : 0));

        if (cursorFingerprint.HasValue)
            writer.WriteU64(cursorFingerprint.Value);

        writer.WriteU8((byte)(capturedWatermark.HasValue ? 1 : 0));

        if (capturedWatermark.HasValue)
            writer.WriteU64(capturedWatermark.Value);


        var response = session.CanBeCanceled && _connection is not null
            ? await _connection.ExecuteWithRetryAsync(RetryOperations.StreamRead,
                token => _connection.RequestOnSessionAsync(MessageTypes.StreamRead, writer.WrittenMemory, session, token), ct).ConfigureAwait(false)
            : await RequestWithRetryAsync(RetryOperations.StreamRead, MessageTypes.StreamRead, writer.WrittenMemory, ct).ConfigureAwait(false);
        return StreamWireHelpers.ReadReadPage(StreamWireHelpers.ReadOptionalPayload(response, "READ"), "READ", route);
    }

    /// <inheritdoc />
    public async Task<StreamRecord?> PeekAsync(string route, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ValidateExactStreamRoute(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var response = await RequestWithRetryAsync(
            RetryOperations.StreamLast,
            MessageTypes.StreamLast,
            writer.WrittenMemory,
            ct).ConfigureAwait(false);
        var data = StreamWireHelpers.ReadOptionalPayload(response, "LAST");

        if (data.IsEmpty)
        {
            return null;
        }


        return StreamWireHelpers.ReadRecord(data, "LAST", route);
    }

    /// <inheritdoc />
    public async Task<StreamMetadata> MetadataAsync(string route, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ValidateExactStreamRoute(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var response = await RequestWithRetryAsync(
            RetryOperations.StreamMetadata,
            MessageTypes.StreamGetMetadata,
            writer.WrittenMemory,
            ct).ConfigureAwait(false);
        var data = StreamWireHelpers.ReadOptionalPayload(response, "METADATA");

        if (data.IsEmpty)
        {
            return new StreamMetadata(0, 0, 0);
        }

        var inner = new BinaryBufferReader(data);
        var firstOffset = StreamWireHelpers.ReadOptionalU64(inner, "METADATA", "first offset") ?? 0;
        var lastOffset = StreamWireHelpers.ReadOptionalU64(inner, "METADATA", "last offset") ?? 0;

        if (inner.RemainingBytes < 24)
        {
            throw new StreamException("METADATA response missing metadata payload", "METADATA_INVALID_RESPONSE");
        }


        var recordCount = inner.ReadU64();
        _ = inner.ReadU64();
        _ = inner.ReadU64();
        _ = StreamWireHelpers.ReadOptionalU64(inner, "METADATA", "ttl seconds");

        if (inner.RemainingBytes < 16)
        {
            throw new StreamException("METADATA response missing metadata payload", "METADATA_INVALID_RESPONSE");
        }

        _ = inner.ReadU64();
        _ = inner.ReadU64();

        if (!inner.IsEof)
        {
            throw new StreamException("METADATA response has trailing bytes", "METADATA_INVALID_RESPONSE");
        }

        return new StreamMetadata(firstOffset, lastOffset, recordCount);
    }

    /// <inheritdoc />
    public Task<StreamSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default
    )
    {
        ThrowIfDisposed();
        EnsureSubscriptionPattern(pattern);
        return _subscriptions.SubscribeReaderAsync(
            pattern,
            static (pattern, notifications, unsubscribe, completion, time) => new StreamSubscription(pattern, notifications, unsubscribe, completion, time),
            ct);
    }

    internal async Task<StreamSubscription> SubscribeAsync(
        string pattern,
        Func<StreamCommitEvent, CancellationToken, ValueTask> handler,
        CancellationToken ct = default
    )
    {
        EnsureSubscriptionPattern(pattern);
        var binding = await _subscriptions.SubscribeAsync(pattern, handler, preDispatch: null, ct).ConfigureAwait(false);
        return new StreamSubscription(pattern, binding.Unsubscribe, binding.Completion, _subscriptions.Time);
    }

    static void EnsureSubscriptionPattern(string pattern)
    {
        if (!RouteValidation.IsStreamSelector(pattern))
        {
            throw new ArgumentException($"pattern '{pattern}' must use whole-segment wildcards and match a three-segment stream route", nameof(pattern));
        }
    }

    async Task<ulong> SubscribeWireAsync(string pattern, SubscriptionSession session, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await session.RequestAsync(_request, MessageTypes.StreamSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        return StreamWireHelpers.ReadSubscriptionId(response);
    }

    async Task UnsubscribeWireAsync(string pattern, ulong subscriptionId, SubscriptionSession session, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await session.RequestAsync(_request, MessageTypes.StreamUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        StreamWireHelpers.EnsureSuccessStatusOnly(response, "UNSUBSCRIBE");
    }

    static DecodedNotification<StreamCommitEvent>? DecodeNotification(ReadOnlyMemory<byte> payload)
    {
        var reader = new BinaryBufferReader(payload);
        var subscriptionId = reader.ReadU64();
        var route = reader.ReadString();
        var bodyLength = reader.ReadU32();
        var body = reader.ReadSpan(bodyLength);

        if (!reader.IsEof)
            return null;


        var commitOffset = StreamWireHelpers.ParseCommitOffset(body);
        var notification = new StreamCommitEvent(route, commitOffset.Offset)
        {
            CommitOffsetStatus = commitOffset.Status,
        };
        return new DecodedNotification<StreamCommitEvent>(subscriptionId, route, notification);
    }

    static void ValidateExactStreamRoute(string route)
    {
        if (RouteValidation.TryValidateFixedRoute(route, "stream", 3, out var failure))
        {
            return;
        }

        ThrowInvalidExactRoute(route, failure);
    }

    static void ThrowInvalidExactRoute(string route, RouteValidationFailure failure)
    {
        var message = failure switch
        {
            RouteValidationFailure.InvalidScheme => $"stream route '{route}' must start with stream://",
            RouteValidationFailure.EmptySegment => $"stream route '{route}' segments must be non-empty",
            _ => $"stream route '{route}' must be stream://{{realm}}/{{area}}/{{resource}}",
        };

        throw new ArgumentException(message, nameof(route));
    }

    ValueTask<ReadOnlyMemory<byte>> RequestWithRetryAsync(
        RetryOperation operation,
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct
    )
    {
        if (_retryRequest is null)
        {
            return _request(messageType, payload, ct);
        }

        return _retryRequest(operation, messageType, payload, ct);
    }

    /// <summary>Releases local resources and ends any registrations this client owns.</summary>
    /// <returns>A task that completes once an in-progress subscription change has finished.</returns>
    /// <remarks>Queued and later subscription changes fail with <see cref="ObjectDisposedException"/>;
    /// unsubscribes and reconnect restoration become no-ops.</remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, true))
        {
            return;
        }

        _connectionBinding?.Dispose();
        await _subscriptions.DisposeAsync().ConfigureAwait(false);
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
}
