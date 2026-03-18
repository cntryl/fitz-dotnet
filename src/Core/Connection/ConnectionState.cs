namespace Cntryl.Fitz.Connection;

public enum ConnectionState
{
    Disconnected,
    Connecting,
    Reconnecting,
    Authenticating,
    Authenticated,
    Closed,
}