using System.Globalization;

namespace Cntryl.Fitz.Transport;

/// <summary>
/// Creates the built-in transport matching a configuration.
/// </summary>
public static class TransportResolver
{
    /// <summary>Validates the configuration and creates the transport it selects.</summary>
    /// <param name="config">Configuration to resolve.</param>
    /// <returns>A transport that is not yet connected.</returns>
    /// <exception cref="NotSupportedException">The configured transport is not supported.</exception>
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
            _ => throw new NotSupportedException($"Transport '{Describe(config.Transport)}' is not supported."),
        };
    }

    // Enum.ToString resolves names through runtime metadata; an explicit map keeps the
    // enum name tables trimmable and the message allocation-free of reflection.
    static string Describe(ClientTransport transport) => transport switch
    {
        ClientTransport.Auto => nameof(ClientTransport.Auto),
        ClientTransport.WebSocket => nameof(ClientTransport.WebSocket),
        ClientTransport.Tcp => nameof(ClientTransport.Tcp),
        _ => ((int)transport).ToString(CultureInfo.InvariantCulture),
    };

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
