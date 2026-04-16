namespace Cntryl.Fitz.Connection;

public enum ConnectionState
{
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Authenticating,
    Authenticated,
    Closed,
}