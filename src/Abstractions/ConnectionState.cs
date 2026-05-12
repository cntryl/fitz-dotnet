namespace Cntryl.Fitz;

/// <summary>
/// Represents the connection lifecycle state of a Fitz client.
/// </summary>
public enum ConnectionState
{
    /// <summary>
    /// No active transport connection is established.
    /// </summary>
    Disconnected,

    /// <summary>
    /// The client is opening a transport connection.
    /// </summary>
    Connecting,

    /// <summary>
    /// The transport connection is established.
    /// </summary>
    Connected,

    /// <summary>
    /// The client is attempting to restore connectivity after disconnect.
    /// </summary>
    Reconnecting,

    /// <summary>
    /// The client is performing broker authentication.
    /// </summary>
    Authenticating,

    /// <summary>
    /// The client is connected and authenticated.
    /// </summary>
    Authenticated,

    /// <summary>
    /// The client was closed by the caller.
    /// </summary>
    Closed,
}