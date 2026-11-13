using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Notice;

/// <summary>
/// The default <see cref="INoticeClient"/>: notice publish and subscribe.
/// </summary>
/// <remarks>
/// Obtained from <see cref="FitzClient"/> rather than constructed directly. The public
/// constructors exist for testing against a transport delegate.
/// </remarks>
sealed class NoticeClient : INoticeClient, IAsyncDisposable
{
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> _send;
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _request;
    readonly Func<ushort, ReadOnlyMemory<byte>, byte, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _requestWithFlags;
    int _disposed;
    readonly SubscriptionTable<NoticeMessage> _subscriptions;
    readonly IDisposable? _connectionBinding;

    internal NoticeClient(FitzConnection connection)
        : this(
            connection.SendAsync,
            connection.RequestAsync,
            connection.RegisterNotificationHandler,
            (handler, rejected) => connection.TryDispatchAsyncHandler("notice", handler, rejected),
            connection.SubscriptionBufferCapacity,
            connection.RequestWithFlagsAsync,
            connection.Time)
    {
        _connectionBinding = _subscriptions.AttachTo(connection);
    }

    /// <summary>
    /// Creates a domain client over a request delegate, for testing without a broker.
    /// </summary>
    public NoticeClient(
        Func<ushort, byte[], CancellationToken, Task> send,
        Func<ushort, byte[], CancellationToken, Task<byte[]>>? request = null,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
        : this(
            async (messageType, payload, ct) => await send(messageType, payload.ToArray(), ct).ConfigureAwait(false),
            request is null
                ? null
                : async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)),
            NotificationRegistrationAdapter.Adapt(registerNotificationHandler))
    {
        ArgumentNullException.ThrowIfNull(send);
    }

    internal NoticeClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? request = null,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null,
        AsyncHandlerDispatch? dispatchAsyncHandler = null,
        int subscriptionBufferCapacity = SubscriptionRegistration<NoticeMessage>.DefaultCapacity,
        Func<ushort, ReadOnlyMemory<byte>, byte, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? requestWithFlags = null,
        TimeProvider? timeProvider = null)
    {
        _send = send;
        _request = request;
        _requestWithFlags = requestWithFlags;
        _subscriptions = new SubscriptionTable<NoticeMessage>(
            this,
            "notice",
            MessageTypes.NoticeNotify,
            registerNotificationHandler,
            dispatchAsyncHandler,
            subscriptionBufferCapacity,
            SubscribeWireAsync,
            UnsubscribeWireAsync,
            DecodeNotification,
            timeProvider);
    }

    /// <inheritdoc />
    public async Task PublishAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (!RouteValidation.IsFixedRoute(route, "notice", 3))
        {
            throw new ArgumentException($"route '{route}' must be notice://{{realm}}/{{area}}/{{resource}}", nameof(route));
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteLengthPrefixed(body.Span);
        await _send(MessageTypes.NoticePublish, writer.WrittenMemory, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task PublishWithAckAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (!RouteValidation.IsFixedRoute(route, "notice", 3))
        {
            throw new ArgumentException($"route '{route}' must be notice://{{realm}}/{{area}}/{{resource}}", nameof(route));
        }
        if (_requestWithFlags is null)
        {
            throw new InvalidOperationException("Request support with frame flags is required for acknowledged notice publish");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteLengthPrefixed(body.Span);
        var response = await _requestWithFlags(
            MessageTypes.NoticePublish,
            writer.WrittenMemory,
            FrameCodec.AckRequestedFlag,
            ct).ConfigureAwait(false);
        var reader = NoticeWireHelpers.ReadSuccess(response, "PUBLISH");
        if (!reader.IsEof)
        {
            throw new NoticeException("PUBLISH acknowledgement has trailing bytes", "PUBLISH_INVALID_RESPONSE");
        }
    }

    /// <inheritdoc />
    public Task<NoticeSubscription> SubscribeAsync(string pattern, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsurePattern(pattern);
        return _subscriptions.SubscribeBufferedAsync(
            pattern,
            static (pattern, notifications, unsubscribe, completion, time) => new NoticeSubscription(pattern, notifications, unsubscribe, completion, time),
            preDispatch: null,
            ct);
    }

    internal async Task<NoticeSubscription> SubscribeAsync(string pattern, Func<NoticeMessage, CancellationToken, ValueTask> handler, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsurePattern(pattern);
        var binding = await _subscriptions.SubscribeAsync(pattern, handler, preDispatch: null, ct).ConfigureAwait(false);
        return new NoticeSubscription(pattern, binding.Unsubscribe, binding.Completion, _subscriptions.Time);
    }

    /// <inheritdoc />
    public Task UnsubscribeAllAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (_request is null)
        {
            throw new InvalidOperationException("Request support is required for notice unsubscription");
        }

        return _subscriptions.RemoveAllAsync(UnsubscribeAllWireAsync, ct);
    }

    static void EnsurePattern(string pattern)
    {
        if (!RouteValidation.IsRegistrationPattern(pattern, "notice"))
        {
            throw new ArgumentException($"pattern '{pattern}' must use whole-segment * or ** wildcards", nameof(pattern));
        }
    }

    async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        if (_request is null)
        {
            throw new InvalidOperationException("Request support is required for notice subscriptions");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.NoticeSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = NoticeWireHelpers.ReadSuccess(response, "SUBSCRIBE");

        if (reader.IsEof || reader.ReadU8() != 1 || reader.RemainingBytes < 8)
        {
            throw new NoticeException("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID");
        }

        var subscriptionId = reader.ReadU64();
        if (!reader.IsEof)
        {
            throw new NoticeException("SUBSCRIBE response has trailing bytes", "SUBSCRIBE_INVALID_RESPONSE");
        }

        return subscriptionId;
    }

    // NOTICE unsubscribes by subscription id; the pattern is not on the wire.
    async Task UnsubscribeWireAsync(string pattern, ulong subscriptionId, CancellationToken ct)
    {
        if (_request is null)
        {
            throw new InvalidOperationException("Request support is required for notice unsubscription");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteU64(subscriptionId);

        var response = await _request(MessageTypes.NoticeUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = NoticeWireHelpers.ReadSuccess(response, "UNSUBSCRIBE");
        if (!reader.IsEof)
        {
            throw new NoticeException("UNSUBSCRIBE response has trailing bytes", "UNSUBSCRIBE_INVALID_RESPONSE");
        }
    }

    async Task UnsubscribeAllWireAsync(CancellationToken ct)
    {
        var response = await _request!(MessageTypes.NoticeUnsubscribeAll, ReadOnlyMemory<byte>.Empty, ct).ConfigureAwait(false);
        var reader = NoticeWireHelpers.ReadSuccess(response, "UNSUBSCRIBE_ALL");
        if (!reader.IsEof)
        {
            throw new NoticeException("UNSUBSCRIBE_ALL response has trailing bytes", "UNSUBSCRIBE_ALL_INVALID_RESPONSE");
        }
    }

    static DecodedNotification<NoticeMessage>? DecodeNotification(ReadOnlyMemory<byte> payload)
    {
        var reader = new BinaryBufferReader(payload);
        var subscriptionId = reader.ReadU64();
        var route = reader.ReadString();
        var bodyLength = reader.ReadU32();
        var body = reader.ReadBytes(bodyLength);
        return reader.IsEof
            ? new DecodedNotification<NoticeMessage>(subscriptionId, route, new NoticeMessage(route, body))
            : null;
    }

    /// <summary>Releases local resources and ends any registrations this client owns.</summary>
    /// <returns>A task that completes once an in-progress subscription change has finished.</returns>
    /// <remarks>Queued and later subscription changes fail with <see cref="ObjectDisposedException"/>;
    /// unsubscribes and reconnect restoration become no-ops.</remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _connectionBinding?.Dispose();
        await _subscriptions.DisposeAsync().ConfigureAwait(false);
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
}
