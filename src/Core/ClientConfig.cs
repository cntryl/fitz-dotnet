using Cntryl.Fitz.Transport;
using Cntryl.Fitz.Observability;

namespace Cntryl.Fitz;

public sealed record ClientConfig(
    string Url,
    string Transport = "auto",
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
    public ClientConfig(
        string Url,
        ClientTransport Transport,
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
        Func<ClientConfig, ITransport>? TransportFactory = null)
        : this(
            Url,
            NormalizeTransport(Transport),
            Timeout,
            AuthSettleDelay,
            TokenProvider,
            Reconnect,
            Retry,
            Heartbeat,
            WebSocket,
            Observability,
            AsyncHandlers,
            MaxFrameSize,
            MaxInFlightRequests,
            MaxRequestQueueSize,
            TransportFactory)
    {
    }

    public ClientTransport TransportKind => ParseTransport(Transport);
    public ClientTransport ResolvedTransportKind => ResolveTransportKind(Url, TransportKind);
    public ReconnectOptions ResolvedReconnect => Reconnect ?? new ReconnectOptions();
    public RetryOptions ResolvedRetry => Retry ?? new RetryOptions();
    public HeartbeatOptions ResolvedHeartbeat => Heartbeat ?? new HeartbeatOptions();
    public AsyncHandlerOptions ResolvedAsyncHandlers => AsyncHandlers ?? new AsyncHandlerOptions();
    public int ResolvedMaxRequestQueueSize => Math.Max(0, MaxRequestQueueSize);

    private static string NormalizeTransport(ClientTransport transport) => transport switch
    {
        ClientTransport.Auto => "auto",
        ClientTransport.WebSocket => "ws",
        ClientTransport.Tcp => "tcp",
        _ => throw new NotSupportedException($"Transport '{transport}' is not supported."),
    };

    private static ClientTransport ParseTransport(string transport) => transport.ToLowerInvariant() switch
    {
        "auto" => ClientTransport.Auto,
        "ws" or "websocket" => ClientTransport.WebSocket,
        "wss" => ClientTransport.WebSocket,
        "http" => ClientTransport.WebSocket,
        "https" => ClientTransport.WebSocket,
        "tcp" => ClientTransport.Tcp,
        _ => throw new NotSupportedException($"Transport '{transport}' is not supported."),
    };

    private static ClientTransport ResolveTransportKind(string url, ClientTransport configuredTransport)
    {
        if (configuredTransport != ClientTransport.Auto)
        {
            return configuredTransport;
        }

        if (!url.Contains("://", StringComparison.Ordinal) && Uri.TryCreate($"tcp://{url}", UriKind.Absolute, out var hostPort))
        {
            if (!string.IsNullOrWhiteSpace(hostPort.Host) && hostPort.Port > 0)
            {
                return ClientTransport.Tcp;
            }
        }

        if (Uri.TryCreate(url, UriKind.Absolute, out var absolute))
        {
            return absolute.Scheme.ToLowerInvariant() switch
            {
                "ws" or "wss" or "http" or "https" => ClientTransport.WebSocket,
                "tcp" => ClientTransport.Tcp,
                _ => throw new NotSupportedException($"URL scheme '{absolute.Scheme}' is not supported for Fitz transport auto-detection."),
            };
        }

        throw new NotSupportedException($"Transport 'auto' could not infer a supported Fitz transport from URL '{url}'.");
    }
}
