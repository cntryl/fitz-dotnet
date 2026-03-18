namespace Cntryl.Fitz.Transport;

public static class TransportResolver
{
    public static ITransport Resolve(ClientConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        return config.Transport.ToLowerInvariant() switch
        {
            "ws" or "websocket" => new WebSocketTransport(config.Url, config.Timeout ?? TimeSpan.FromSeconds(30)),
            _ => throw new NotSupportedException($"Transport '{config.Transport}' is not supported yet."),
        };
    }
}