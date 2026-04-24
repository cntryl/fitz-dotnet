namespace Cntryl.Fitz;

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