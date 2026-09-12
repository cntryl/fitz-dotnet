using Cntryl.Fitz.Observability;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz;

/// <summary>
/// The complete configuration for a <see cref="Client"/>.
/// </summary>
/// <param name="Url">
/// Broker endpoint. The scheme selects the transport when <paramref name="Transport"/> is
/// <see cref="ClientTransport.Auto"/>. Must be absolute.
/// </param>
/// <param name="Transport">Transport override. Defaults to selecting from the URL scheme.</param>
/// <param name="Timeout">Per-request deadline. Defaults to 30 seconds.</param>
/// <param name="AuthSettleDelay">
/// Compatibility window the client waits after sending credentials. The protocol has no
/// positive authentication acknowledgement, so a rejection arriving within this window is
/// treated as authoritative.
/// </param>
/// <param name="TokenProvider">
/// Supplies a bearer token per connection attempt. Invoked again on every reconnect, so it
/// can return a freshly minted token.
/// </param>
/// <param name="Reconnect">Automatic reconnection behavior. Enabled by default.</param>
/// <param name="Retry">Automatic per-request retry behavior. Enabled by default.</param>
/// <param name="Heartbeat">Transport keepalive behavior. Enabled by default.</param>
/// <param name="WebSocket">WebSocket-specific settings, such as upgrade headers.</param>
/// <param name="Observability">Logging, tracing, metrics, and lifecycle hooks.</param>
/// <param name="AsyncHandlers">Bounds on callback dispatch and subscription buffering.</param>
/// <param name="MaxFrameSize">
/// Largest transport frame accepted or produced. The Fitz payload length is 16-bit, and a
/// correlated frame includes its label, so this is bounded by the protocol: see
/// <see cref="FitzLimits.MinFrameSize"/> and <see cref="FitzLimits.MaxFrameSize"/>.
/// </param>
/// <param name="MaxInFlightRequests">Maximum requests awaiting a response at once.</param>
/// <param name="MaxRequestQueueSize">
/// Depth of the pending-request queue. Exceeding it raises <c>RequestQueueFullException</c>.
/// </param>
/// <param name="TransportFactory">
/// Supplies a custom <see cref="ITransport"/> instead of the built-in ones. Override
/// <see cref="ITransport.TransportName"/> to label it in telemetry.
/// </param>
/// <remarks>
/// This record is validated centrally before any connection work starts, so invalid
/// configuration fails fast rather than at first use.
/// </remarks>
public sealed record ClientConfig(
    Uri Url,
    ClientTransport Transport = ClientTransport.Auto,
    TimeSpan? Timeout = null,
    TimeSpan? AuthSettleDelay = null,
    Func<CancellationToken, ValueTask<string>>? TokenProvider = null,
    ReconnectOptions? Reconnect = null,
    RetryOptions? Retry = null,
    HeartbeatOptions? Heartbeat = null,
    WebSocketOptions? WebSocket = null,
    FitzObservabilityOptions? Observability = null,
    AsyncHandlerOptions? AsyncHandlers = null,
    int MaxFrameSize = FitzLimits.MaxFrameSize,
    int MaxInFlightRequests = 256,
    int MaxRequestQueueSize = 1024,
    Func<ClientConfig, ITransport>? TransportFactory = null
)
{
    static readonly ReconnectOptions DefaultReconnect = new();
    static readonly RetryOptions DefaultRetry = new();
    static readonly HeartbeatOptions DefaultHeartbeat = new();
    static readonly AsyncHandlerOptions DefaultAsyncHandlers = new();

    /// <summary>The transport actually used, with <see cref="ClientTransport.Auto"/> resolved against the URL scheme.</summary>
    public ClientTransport ResolvedTransportKind => ResolveTransportKind(Url, Transport);
    /// <summary>Reconnection settings, falling back to a shared default instance.</summary>
    public ReconnectOptions ResolvedReconnect => Reconnect ?? DefaultReconnect;
    /// <summary>Retry settings, falling back to a shared default instance.</summary>
    public RetryOptions ResolvedRetry => Retry ?? DefaultRetry;
    /// <summary>Keepalive settings, falling back to a shared default instance.</summary>
    public HeartbeatOptions ResolvedHeartbeat => Heartbeat ?? DefaultHeartbeat;
    /// <summary>Callback dispatch settings, falling back to a shared default instance.</summary>
    public AsyncHandlerOptions ResolvedAsyncHandlers => AsyncHandlers ?? DefaultAsyncHandlers;
    /// <summary>The effective pending-request queue depth.</summary>
    public int ResolvedMaxRequestQueueSize => MaxRequestQueueSize;

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Url);
        if (!Url.IsAbsoluteUri)
        {
            throw new ArgumentException("The Fitz URL must be absolute.", nameof(Url));
        }
        if (Transport is not ClientTransport.Auto and not ClientTransport.WebSocket and not ClientTransport.Tcp)
        {
            throw new ArgumentOutOfRangeException(nameof(Transport), Transport, "Unknown client transport.");
        }

        ValidatePositiveTimeout(Timeout, nameof(Timeout));
        if (AuthSettleDelay is { } authSettleDelay && authSettleDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(AuthSettleDelay), "Authentication settlement delay cannot be negative.");
        }

        if (MaxFrameSize is < FitzLimits.MinFrameSize or > FitzLimits.MaxFrameSize)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxFrameSize),
                $"MaxFrameSize must be between {FitzLimits.MinFrameSize} and {FitzLimits.MaxFrameSize}; the Fitz wire payload length is 16-bit and a correlated transport frame includes its label.");
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxInFlightRequests);
        ArgumentOutOfRangeException.ThrowIfNegative(MaxRequestQueueSize);
        ValidateRetry(ResolvedRetry.Enabled, ResolvedRetry.MaxAttempts, ResolvedRetry.Backoff, ResolvedRetry.MaxBackoff, nameof(Retry));
        ValidateRetry(ResolvedReconnect.Enabled, ResolvedReconnect.MaxAttempts, ResolvedReconnect.Backoff, ResolvedReconnect.MaxBackoff, nameof(Reconnect));
        ValidatePositiveTimeout(ResolvedHeartbeat.Interval, $"{nameof(Heartbeat)}.{nameof(HeartbeatOptions.Interval)}");
        ValidatePositiveTimeout(ResolvedHeartbeat.Timeout, $"{nameof(Heartbeat)}.{nameof(HeartbeatOptions.Timeout)}");
        ValidatePositiveTimeout(ResolvedAsyncHandlers.Timeout, $"{nameof(AsyncHandlers)}.{nameof(AsyncHandlerOptions.Timeout)}");
        if (ResolvedAsyncHandlers.MaxConcurrency is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(AsyncHandlers), "MaxConcurrency must be positive when configured.");
        }

        ArgumentOutOfRangeException.ThrowIfNegative(ResolvedAsyncHandlers.QueueCapacity, $"{nameof(AsyncHandlers)}.{nameof(AsyncHandlerOptions.QueueCapacity)}");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ResolvedAsyncHandlers.SubscriptionBufferCapacity, $"{nameof(AsyncHandlers)}.{nameof(AsyncHandlerOptions.SubscriptionBufferCapacity)}");
        _ = ResolvedTransportKind;
    }

    static ClientTransport ResolveTransportKind(Uri url, ClientTransport configuredTransport)
    {
        if (configuredTransport != ClientTransport.Auto)
        {
            return configuredTransport;
        }

        if (!url.IsAbsoluteUri)
        {
            throw new NotSupportedException($"Transport 'auto' requires an absolute Fitz URL, but received '{url}'.");
        }

        return url.Scheme.ToUpperInvariant() switch
        {
            "WS" or "WSS" or "HTTP" or "HTTPS" => ClientTransport.WebSocket,
            "TCP" => ClientTransport.Tcp,
            _ => throw new NotSupportedException($"URL scheme '{url.Scheme}' is not supported for Fitz transport auto-detection."),
        };
    }

    static void ValidateRetry(bool enabled, int maxAttempts, TimeSpan? backoff, TimeSpan? maxBackoff, string parameterName)
    {
        if (enabled && maxAttempts <= 0)
        {
            throw new ArgumentOutOfRangeException(parameterName, "MaxAttempts must be positive when retry is enabled.");
        }

        ValidateNonnegativeDelay(backoff, $"{parameterName}.Backoff");
        ValidateNonnegativeDelay(maxBackoff, $"{parameterName}.MaxBackoff");
        if (backoff.HasValue && maxBackoff.HasValue &&
            maxBackoff != System.Threading.Timeout.InfiniteTimeSpan &&
            (backoff == System.Threading.Timeout.InfiniteTimeSpan || backoff > maxBackoff))
        {
            throw new ArgumentOutOfRangeException(parameterName, "Backoff cannot exceed MaxBackoff.");
        }
    }

    static void ValidatePositiveTimeout(TimeSpan? value, string parameterName)
    {
        if (value is { } configured && configured != System.Threading.Timeout.InfiniteTimeSpan && configured <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(parameterName, "The configured duration must be positive or Timeout.InfiniteTimeSpan.");
        }
    }

    static void ValidateNonnegativeDelay(TimeSpan? value, string parameterName)
    {
        if (value is { } configured && configured != System.Threading.Timeout.InfiniteTimeSpan && configured < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(parameterName, "The configured delay cannot be negative.");
        }
    }
}
