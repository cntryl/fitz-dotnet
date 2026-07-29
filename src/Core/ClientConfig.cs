using Cntryl.Fitz.Transport;
using Cntryl.Fitz.Observability;

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
    int MaxFrameSize = 64 * 1024,
    int MaxInFlightRequests = 256,
    int MaxRequestQueueSize = 1024,
    Func<ClientConfig, ITransport>? TransportFactory = null
)
{
    public ClientTransport ResolvedTransportKind => ResolveTransportKind(Url, Transport);
    public ReconnectOptions ResolvedReconnect => Reconnect ?? new ReconnectOptions();
    public RetryOptions ResolvedRetry => Retry ?? new RetryOptions();
    public HeartbeatOptions ResolvedHeartbeat => Heartbeat ?? new HeartbeatOptions();
    public AsyncHandlerOptions ResolvedAsyncHandlers => AsyncHandlers ?? new AsyncHandlerOptions();
    public int ResolvedMaxRequestQueueSize => Math.Max(0, MaxRequestQueueSize);

    private static ClientTransport ResolveTransportKind(Uri url, ClientTransport configuredTransport)
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
}
