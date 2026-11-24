using Cntryl.Fitz.Observability;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz;

/// <summary>
/// The complete configuration for a <see cref="FitzClient"/>.
/// </summary>
/// <param name="Url">
/// Broker endpoint. The scheme selects the transport when <see cref="Transport"/> is
/// <see cref="ClientTransport.Auto"/>. Must be absolute.
/// </param>
/// <remarks>
/// This record is validated centrally before any connection work starts, so invalid
/// configuration fails fast rather than at first use.
/// </remarks>
public sealed record ClientConfig(Uri Url)
{
    /// <summary>Transport override. Defaults to selecting from the URL scheme.</summary>
    public ClientTransport Transport { get; init; } = ClientTransport.Auto;

    /// <summary>Per-request deadline. Defaults to 30 seconds.</summary>
    public TimeSpan? Timeout { get; init; }

    /// <summary>Supplies a bearer token per connection attempt. Invoked again on every reconnect, so it
    /// can return a freshly minted token.</summary>
    public Func<CancellationToken, ValueTask<string>>? TokenProvider { get; init; }

    /// <summary>Automatic reconnection behavior. Enabled by default.</summary>
    public ReconnectOptions? Reconnect { get; init; }

    /// <summary>Automatic per-request retry behavior. Enabled by default.</summary>
    public RetryOptions? Retry { get; init; }

    /// <summary>Transport keepalive behavior. Enabled by default.</summary>
    public HeartbeatOptions? Heartbeat { get; init; }

    /// <summary>WebSocket-specific settings, such as upgrade headers.</summary>
    public WebSocketOptions? WebSocket { get; init; }

    /// <summary>Logging, tracing, metrics, and lifecycle hooks.</summary>
    public FitzObservabilityOptions? Observability { get; init; }

    /// <summary>Bounds on callback dispatch and subscription buffering.</summary>
    public AsyncHandlerOptions? AsyncHandlers { get; init; }

    /// <summary>Largest transport frame accepted or produced, including the fixed Wire 3 header. This is
    /// bounded by the protocol: see
    /// <see cref="FitzLimits.MinFrameSize"/> and <see cref="FitzLimits.MaxFrameSize"/>.</summary>
    public int MaxFrameSize { get; init; } = FitzLimits.MaxFrameSize;

    /// <summary>Maximum requests awaiting a response at once.</summary>
    public int MaxInFlightRequests { get; init; } = 256;

    /// <summary>Depth of the pending-request queue. Exceeding it raises <c>RequestQueueFullException</c>.</summary>
    public int MaxRequestQueueSize { get; init; } = DefaultRequestQueueSize;

    internal const int DefaultRequestQueueSize = 1024;

    /// <summary>Supplies a custom <see cref="ITransport"/> instead of the built-in ones. Override
    /// <see cref="ITransport.TransportName"/> to label it in telemetry.</summary>
    public Func<ClientConfig, ITransport>? TransportFactory { get; init; }

    /// <summary>Optional friendly service name included in the CONNECT payload.</summary>
    public string? ServiceName { get; init; }

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
        if (ServiceName is { } serviceName)
        {
            var normalizedServiceName = serviceName.Trim();
            var utf8 = new System.Text.UTF8Encoding(false, true);
            int byteCount;
            try
            {
                byteCount = utf8.GetByteCount(normalizedServiceName);
            }
            catch (System.Text.EncoderFallbackException exception)
            {
                throw new ArgumentException("ServiceName must contain valid Unicode characters.", nameof(ServiceName), exception);
            }
            if (normalizedServiceName.Length == 0 || byteCount > 128)
            {
                throw new ArgumentException("ServiceName must be non-empty and at most 128 UTF-8 bytes.", nameof(ServiceName));
            }
            if (normalizedServiceName.Any(char.IsControl))
            {
                throw new ArgumentException("ServiceName must not contain control characters.", nameof(ServiceName));
            }
        }
        if (!Url.IsAbsoluteUri)
        {
            throw new ArgumentException("The Fitz URL must be absolute.", nameof(Url));
        }
        if (Transport is not ClientTransport.Auto and not ClientTransport.WebSocket and not ClientTransport.Tcp)
        {
            throw new ArgumentOutOfRangeException(nameof(Transport), Transport, "Unknown client transport.");
        }

        ValidatePositiveTimeout(Timeout, nameof(Timeout));
        if (MaxFrameSize is < FitzLimits.MinFrameSize or > FitzLimits.MaxFrameSize)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxFrameSize),
                $"MaxFrameSize must be between {FitzLimits.MinFrameSize} and {FitzLimits.MaxFrameSize} bytes, including the Wire 3 header.");
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
