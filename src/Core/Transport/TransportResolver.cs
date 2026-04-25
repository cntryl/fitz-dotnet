namespace Cntryl.Fitz.Transport;

public static class TransportResolver
{
    public static ITransport Resolve(ClientConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        return config.TransportKind switch
        {
            ClientTransport.WebSocket => new WebSocketTransport(config.Url, config.Timeout ?? TimeSpan.FromSeconds(30), config.MaxFrameSize),
            ClientTransport.Tcp => new TcpTransport(config.Url, config.Timeout ?? TimeSpan.FromSeconds(30), config.MaxFrameSize),
            _ => throw new NotSupportedException($"Transport '{config.Transport}' is not supported."),
        };
    }
}
