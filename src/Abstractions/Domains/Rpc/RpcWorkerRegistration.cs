namespace Cntryl.Fitz.Abstractions.Domains.Rpc;

public sealed class RpcWorkerRegistration : IAsyncDisposable
{
    private readonly Func<CancellationToken, ValueTask> _unregister;
    private int _unregistered;

    public RpcWorkerRegistration(
        string pattern,
        Func<CancellationToken, ValueTask> unregister)
    {
        Pattern = pattern;
        _unregister = unregister;
    }

    public string Pattern { get; }

    public ValueTask UnregisterAsync(CancellationToken cancellationToken = default)
    {
        return UnregisterCoreAsync(cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        return UnregisterCoreAsync(CancellationToken.None);
    }

    private async ValueTask UnregisterCoreAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _unregistered, 1) != 0)
        {
            return;
        }

        await _unregister(cancellationToken).ConfigureAwait(false);
    }
}