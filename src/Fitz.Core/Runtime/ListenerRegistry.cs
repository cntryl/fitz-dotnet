namespace Cntryl.Fitz.Runtime;

/// <summary>
/// A thread-safe set of listeners, each removed by disposing the registration returned when it was added.
/// </summary>
/// <typeparam name="TListener">The listener delegate type.</typeparam>
/// <remarks>
/// Callers invoke a <see cref="Snapshot"/> outside any lock, so a listener may add or remove
/// registrations, including its own, while it runs.
/// </remarks>
sealed class ListenerRegistry<TListener>
    where TListener : Delegate
{
    readonly Lock _gate = new();
    readonly Dictionary<long, TListener> _listeners = [];
    long _nextId;

    /// <summary>Adds a listener.</summary>
    /// <param name="listener">The listener to add.</param>
    /// <returns>A registration that removes the listener when disposed.</returns>
    internal IDisposable Add(TListener listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
        long id;
        lock (_gate)
        {
            id = ++_nextId;
            _listeners[id] = listener;
        }

        return new Registration(this, id);
    }

    /// <summary>Returns the current listeners in registration order.</summary>
    /// <returns>A copy safe to iterate without holding a lock.</returns>
    internal TListener[] Snapshot()
    {
        lock (_gate)
        {
            return [.. _listeners.Values];
        }
    }

    void Remove(long id)
    {
        lock (_gate)
        {
            _listeners.Remove(id);
        }
    }

    sealed class Registration(ListenerRegistry<TListener> owner, long id) : IDisposable
    {
        bool _disposed;

        public void Dispose()
        {
            if (!Interlocked.Exchange(ref _disposed, true))
                owner.Remove(id);
        }
    }
}
