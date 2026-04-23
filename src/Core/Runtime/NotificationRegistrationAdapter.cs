namespace Cntryl.Fitz.Runtime;

internal static class NotificationRegistrationAdapter
{
    internal static Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? Adapt(
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler)
    {
        if (registerNotificationHandler is null)
        {
            return null;
        }

        return (messageType, handler) => registerNotificationHandler(messageType, payload => handler(payload));
    }
}