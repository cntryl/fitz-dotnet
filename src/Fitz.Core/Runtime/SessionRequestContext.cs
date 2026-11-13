using Cntryl.Fitz.Connection;

namespace Cntryl.Fitz.Runtime;

// Keeps lifetime registration on the same side of the await as the successful reply.
// Test request delegates use an early listener; real connections return the exact
// transport-session token and bind later handle operations to that session.
sealed class SessionRequestContext : IDisposable
{
    readonly object _gate = new();
    readonly FitzConnection? _connection;
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly Func<Action, IDisposable>? _registerOnDisconnect;
    readonly Dictionary<long, Action> _pendingListeners = [];
    IDisposable? _earlyRegistration;
    CancellationToken _session;
    long _nextId;
    bool _ended;

    internal SessionRequestContext(FitzConnection? connection,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<Action, IDisposable>? registerOnDisconnect)
    {
        _connection = connection;
        _request = request;
        _registerOnDisconnect = registerOnDisconnect;
        if (connection is null)
            _earlyRegistration = registerOnDisconnect?.Invoke(OnDisconnected);
    }

    // extraResponseTime lengthens the connection's response timeout for requests the broker
    // deliberately holds open, such as a Queue RESERVE long poll.
    internal async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        ushort type, ReadOnlyMemory<byte> payload, CancellationToken ct, TimeSpan extraResponseTime = default)
    {
        ReadOnlyMemory<byte> response;
        if (_connection is not null)
        {
            var result = await _connection.RequestInSessionAsync(type, payload, extraResponseTime, ct).ConfigureAwait(false);
            _session = result.Closed;
            response = result.Payload;
        }
        else
        {
            response = await _request(type, payload, ct).ConfigureAwait(false);
        }
        EnsureValid();
        return response;
    }

    internal void EnsureValid()
    {
        lock (_gate)
        {
            if (_ended || _session.IsCancellationRequested)
                throw new ConnectionException("The response belongs to a connection session that has ended.");
        }
    }

    internal Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> BoundRequest
    {
        get
        {
            if (_connection is not { } connection)
                return _request;
            var session = _session;
            return (type, payload, ct) => connection.RequestOnSessionAsync(type, payload, session, ct);
        }
    }

    internal Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? BindRetry(
        Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? retry)
    {
        if (retry is null || _connection is not { } connection)
            return retry;
        var request = BoundRequest;
        return (operation, type, payload, ct) => connection.ExecuteWithRetryAsync(
            operation, token => request(type, payload, token), ct);
    }

    internal IDisposable RegisterOnDisconnect(Action listener)
    {
        if (_connection is not null)
            return _session.Register(listener);

        long id;
        bool ended;
        lock (_gate)
        {
            id = ++_nextId;
            _pendingListeners.Add(id, listener);
            ended = _ended;
        }
        var registration = _registerOnDisconnect?.Invoke(listener);
        if (ended)
            listener();
        return new PendingRegistration(this, id, registration);
    }

    void OnDisconnected()
    {
        Action[] listeners;
        lock (_gate)
        {
            _ended = true;
            listeners = [.. _pendingListeners.Values];
        }
        foreach (var listener in listeners)
        {
            try
            {
                listener();
            }
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
            }
        }
    }

    public void Dispose() => Interlocked.Exchange(ref _earlyRegistration, null)?.Dispose();

    sealed class PendingRegistration(SessionRequestContext owner, long id, IDisposable? registration) : IDisposable
    {
        int _disposed;
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            registration?.Dispose();
            lock (owner._gate)
                owner._pendingListeners.Remove(id);
        }
    }
}
