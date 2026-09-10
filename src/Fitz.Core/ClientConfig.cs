using Cntryl.Fitz.Observability;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz;

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
    int MaxFrameSize = 65_540,
    int MaxInFlightRequests = 256,
    int MaxRequestQueueSize = 1024,
    Func<ClientConfig, ITransport>? TransportFactory = null
)
{
    static readonly ReconnectOptions DefaultReconnect = new();
    static readonly RetryOptions DefaultRetry = new();
    static readonly HeartbeatOptions DefaultHeartbeat = new();
    static readonly AsyncHandlerOptions DefaultAsyncHandlers = new();

    public ClientTransport ResolvedTransportKind => ResolveTransportKind(Url, Transport);
    public ReconnectOptions ResolvedReconnect => Reconnect ?? DefaultReconnect;
    public RetryOptions ResolvedRetry => Retry ?? DefaultRetry;
    public HeartbeatOptions ResolvedHeartbeat => Heartbeat ?? DefaultHeartbeat;
    public AsyncHandlerOptions ResolvedAsyncHandlers => AsyncHandlers ?? DefaultAsyncHandlers;
    public int ResolvedMaxRequestQueueSize => MaxRequestQueueSize;

    internal void Validate()
    {
        if (!Url.IsAbsoluteUri)
        {
            throw new ArgumentException("The Fitz URL must be absolute.", nameof(Url));
        }

        ValidatePositiveTimeout(Timeout, nameof(Timeout));
        if (AuthSettleDelay is { } authSettleDelay && authSettleDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(AuthSettleDelay), "Authentication settlement delay cannot be negative.");
        }

        if (MaxFrameSize is < FrameCodec.MaxHeaderSize or > ushort.MaxValue + FrameCodec.MaxHeaderSize)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxFrameSize),
                $"MaxFrameSize must be between {FrameCodec.MaxHeaderSize} and {ushort.MaxValue + FrameCodec.MaxHeaderSize}; the Fitz wire payload length is a 16-bit value.");
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
