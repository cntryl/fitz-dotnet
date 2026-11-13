namespace Cntryl.Fitz.Runtime;

/// <summary>
/// Tracks whether a handle bound to one broker session is still valid. The latch closes once:
/// when the owner finishes with the handle, or when the connection drops and the session ends.
/// </summary>
/// <remarks>
/// Closing releases the disconnect registration, so a finished handle is not kept alive by the
/// connection. A connection that is already down may fire the disconnect callback during
/// construction; the latch then closes and drops the registration as soon as it is returned.
/// </remarks>
sealed class SessionLatch
{
    IDisposable? _disconnectRegistration;
    bool _closed;

    /// <summary>Creates an open latch.</summary>
    /// <param name="registerOnDisconnect">Connection hook for disconnects, or <see langword="null"/> for an unbound handle.</param>
    /// <param name="onConnectionLost">Runs once if a disconnect, rather than the owner, closed the latch.</param>
    internal SessionLatch(Func<Action, IDisposable>? registerOnDisconnect, Action? onConnectionLost = null)
    {
        var registration = registerOnDisconnect?.Invoke(() =>
        {
            if (TryClose())
                onConnectionLost?.Invoke();
        });
        if (registration is null)
            return;

        Interlocked.Exchange(ref _disconnectRegistration, registration)?.Dispose();
        if (IsClosed)
            Interlocked.Exchange(ref _disconnectRegistration, null)?.Dispose();
    }

    /// <summary>Whether the handle has been closed.</summary>
    internal bool IsClosed => Volatile.Read(ref _closed);

    /// <summary>Closes the latch.</summary>
    /// <returns><see langword="true"/> for the call that closed it; <see langword="false"/> if it was already closed.</returns>
    internal bool TryClose()
    {
        if (Interlocked.Exchange(ref _closed, true))
            return false;

        Interlocked.Exchange(ref _disconnectRegistration, null)?.Dispose();
        return true;
    }
}
