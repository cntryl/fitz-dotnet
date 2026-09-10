namespace Cntryl.Fitz.Abstractions.Domains.Rpc;

[System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "The small semaphore remains valid for concurrent callers racing terminal disposal.")]
public sealed class RpcWorkerRegistration : IAsyncDisposable
{
    readonly Func<CancellationToken, ValueTask> _unregister;
    readonly SemaphoreSlim _unregisterGate = new(1, 1);
    int _unregistered;

    public RpcWorkerRegistration(
        string pattern,
        Func<CancellationToken, ValueTask> unregister)
    {
        Pattern = pattern;
        _unregister = unregister;
    }

    public string Pattern { get; }

    public ValueTask UnregisterAsync(CancellationToken cancellationToken = default) => UnregisterCoreAsync(cancellationToken);

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Worker disposal is bounded best-effort cleanup and must not replace an exception leaving an await-using scope.")]
    public async ValueTask DisposeAsync()
    {
        try
        {
            using var cleanupDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await UnregisterCoreAsync(cleanupDeadline.Token).ConfigureAwait(false);
        }
        catch
        {
        }
    }

    async ValueTask UnregisterCoreAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _unregistered) != 0)
        {
            return;
        }

        await _unregisterGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _unregistered) != 0)
            {
                return;
            }

            await _unregister(cancellationToken).ConfigureAwait(false);
            Volatile.Write(ref _unregistered, 1);
        }
        finally
        {
            _unregisterGate.Release();
        }
    }
}
