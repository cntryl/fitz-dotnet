using System.Runtime.CompilerServices;
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
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
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
        TimeProvider? timeProvider = null)
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
            _time);
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
    public IAsyncEnumerable<StreamRecord> ReadAsync(
        string route,
        ulong startOffset,
        ulong limit = 100,
        StreamFilterSet? filter = null,
        ulong? maxBytes = null,
        ulong? cursorFingerprint = null,
        ulong? capturedWatermark = null,
        CancellationToken ct = default)
    {
        // Validate at the call site, not on the first MoveNextAsync.
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(route);
        ValidateStreamSelector(route);
        _ = RouteValidation.TryGetStreamSelectorScope(route, out var scope);
        return ReadCoreAsync(route, scope, startOffset, limit, filter, maxBytes, cursorFingerprint, capturedWatermark, ct);
    }

    async IAsyncEnumerable<StreamRecord> ReadCoreAsync(
        string route,
        StreamSelectorScope scope,
        ulong startOffset,
        ulong limit,
        StreamFilterSet? filter,
        ulong? maxBytes,
        ulong? cursorFingerprint,
        ulong? capturedWatermark,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var nextOffset = startOffset;
        var fingerprint = cursorFingerprint;
        var watermark = capturedWatermark;

        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var page = await ReadPageAsync(route, nextOffset, limit, filter, maxBytes, fingerprint, watermark, ct).ConfigureAwait(false);
            foreach (var item in page.Items)
            {
                if (item.Kind == StreamReadItemKind.Event && item.Record is not null)
                {
                    yield return item.Record;
                }
            }

            if (!page.Cursor.HasMore)
            {
                yield break;
            }

            var lastOffset = scope switch
            {
                StreamSelectorScope.Resource => page.Cursor.LastResourceOffset,
                StreamSelectorScope.Area => page.Cursor.LastAreaOffset,
                StreamSelectorScope.Realm => page.Cursor.LastRealmOffset,
                StreamSelectorScope.Global => page.Cursor.LastGlobalOffset,
                _ => null,
            };
            if (!lastOffset.HasValue || lastOffset.Value < nextOffset || lastOffset.Value == ulong.MaxValue)
            {
                throw new StreamException("READ response contains a non-progressing continuation cursor", "READ_INVALID_RESPONSE");
            }

            nextOffset = lastOffset.Value + 1;
            if (scope == StreamSelectorScope.Global)
            {
                fingerprint = page.Cursor.CursorFingerprint
                    ?? throw new StreamException("READ response is missing the global cursor fingerprint", "READ_INVALID_RESPONSE");
                watermark = page.Cursor.CapturedWatermark
                    ?? throw new StreamException("READ response is missing the captured watermark", "READ_INVALID_RESPONSE");
            }
        }
    }

    /// <inheritdoc />
    public async Task<StreamReadPage> ReadPageAsync(
        string route,
        ulong startOffset,
        ulong limit = 100,
        StreamFilterSet? filter = null,
        ulong? maxBytes = null,
        ulong? cursorFingerprint = null, ulong? capturedWatermark = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ValidateStreamSelector(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU64(startOffset);
        writer.WriteU64(limit);
        writer.WriteU8((byte)(maxBytes.HasValue ? 1 : 0));
        if (maxBytes.HasValue)
        {
            writer.WriteU64(maxBytes.Value);
        }

        var filterBytes = StreamWireHelpers.EncodeStreamFilterSet(filter);
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

        var response = await RequestWithRetryAsync(
            RetryOperations.StreamRead,
            MessageTypes.StreamRead,
            writer.WrittenMemory,
            ct).ConfigureAwait(false);
        var data = StreamWireHelpers.ReadOptionalPayload(response, "READ");
        return data.IsEmpty ? new StreamReadPage(Array.Empty<StreamReadItem>(), new StreamReadCursor(0, null, null, null, null, null, false)) : StreamWireHelpers.ReadReadPage(data, "READ", route);
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

        return StreamWireHelpers.ReadRoutedRecord(data, "LAST");
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
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsureSubscriptionPattern(pattern);
        return _subscriptions.SubscribeBufferedAsync(
            pattern,
            static (pattern, notifications, unsubscribe, completion, time) => new StreamSubscription(pattern, notifications, unsubscribe, completion, time),
            preDispatch: null,
            ct);
    }

    internal async Task<StreamSubscription> SubscribeAsync(
        string pattern,
        Func<StreamCommitEvent, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
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

    async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.StreamSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        return StreamWireHelpers.ReadSubscriptionId(response);
    }

    async Task UnsubscribeWireAsync(string pattern, ulong subscriptionId, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.StreamUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
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

    static void ValidateStreamSelector(string route)
    {
        if (RouteValidation.IsStreamSelector(route))
        {
            return;
        }

        throw new ArgumentException($"stream selector '{route}' must use the canonical resource, area, realm, or global selector shape", nameof(route));
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
        CancellationToken ct)
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
