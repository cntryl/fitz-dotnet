namespace Cntryl.Fitz;

readonly struct TransportDeadline : IDisposable
{
    readonly CancellationTokenSource? _timeout;
    readonly CancellationTokenSource? _linked;

    internal TransportDeadline(TimeSpan timeout, TimeProvider timeProvider, CancellationToken ct)
    {
        _timeout = timeout == Timeout.InfiniteTimeSpan ? null : new CancellationTokenSource(timeout, timeProvider);
        _linked = _timeout is not null && ct.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(ct, _timeout.Token)
            : null;
        Token = _linked?.Token ?? _timeout?.Token ?? ct;
    }

    internal CancellationToken Token { get; }

    public void Dispose()
    {
        _linked?.Dispose();
        _timeout?.Dispose();
    }
}
