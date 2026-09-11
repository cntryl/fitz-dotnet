namespace Cntryl.Fitz;

/// <summary>
/// Selects the transport the client uses to reach the broker.
/// </summary>
public enum ClientTransport
{
    /// <summary>Choose the transport from the URL scheme. The default.</summary>
    Auto,

    /// <summary>Connect over WebSocket, for <c>ws</c>, <c>wss</c>, <c>http</c>, and <c>https</c> URLs.</summary>
    WebSocket,

    /// <summary>Connect over raw TCP.</summary>
    Tcp,
}
