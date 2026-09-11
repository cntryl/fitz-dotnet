namespace Cntryl.Fitz.Transport;

public static class TransportResolver
{
    public static ITransport Resolve(ClientConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        config.Validate();

        return config.ResolvedTransportKind switch
        {
            ClientTransport.WebSocket => new WebSocketTransport(
                NormalizeWebSocketUrl(config.Url),
                config.Timeout ?? TimeSpan.FromSeconds(30),
                config.MaxFrameSize,
                config.WebSocket,
                config.ResolvedHeartbeat),
            ClientTransport.Tcp => new TcpTransport(
                config.Url,
                config.Timeout ?? TimeSpan.FromSeconds(30),
                config.MaxFrameSize,
                config.ResolvedHeartbeat),
            _ => throw new NotSupportedException($"Transport '{config.Transport}' is not supported."),
        };
    }

    static Uri NormalizeWebSocketUrl(Uri url)
    {
        if (url.Scheme is not ("http" or "https"))
        {
            return url;
        }

        var builder = new UriBuilder(url)
        {
            Scheme = url.Scheme == "https" ? "wss" : "ws",
        };
        return builder.Uri;
    }
}
