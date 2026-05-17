using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz;

public sealed record ClientConfig(
    string Url,
    string Transport = "ws",
    TimeSpan? Timeout = null,
    TimeSpan? AuthSettleDelay = null,
    Func<CancellationToken, ValueTask<string>>? TokenProvider = null,
    ReconnectOptions? Reconnect = null,
    int MaxFrameSize = 64 * 1024,
    int MaxInFlightRequests = 256,
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
        int MaxFrameSize = 64 * 1024,
        int MaxInFlightRequests = 256,
        Func<ClientConfig, ITransport>? TransportFactory = null)
        : this(
            Url,
            NormalizeTransport(Transport),
            Timeout,
            AuthSettleDelay,
            TokenProvider,
            Reconnect,
            MaxFrameSize,
            MaxInFlightRequests,
            TransportFactory)
    {
    }

    public ClientTransport TransportKind => ParseTransport(Transport);

    private static string NormalizeTransport(ClientTransport transport) => transport switch
    {
        ClientTransport.WebSocket => "ws",
        ClientTransport.Tcp => "tcp",
        _ => throw new NotSupportedException($"Transport '{transport}' is not supported."),
    };

    private static ClientTransport ParseTransport(string transport) => transport.ToLowerInvariant() switch
    {
        "ws" or "websocket" => ClientTransport.WebSocket,
        "tcp" => ClientTransport.Tcp,
        _ => throw new NotSupportedException($"Transport '{transport}' is not supported."),
    };
}
