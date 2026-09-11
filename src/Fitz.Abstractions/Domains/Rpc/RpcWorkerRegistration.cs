namespace Cntryl.Fitz.Abstractions.Domains.Rpc;

/// <summary>
/// An active RPC worker registration. Dispose to withdraw it.
/// </summary>
/// <remarks>
/// Unregistering twice is safe. Disposal is bounded best-effort and never replaces an
/// exception already leaving an <c>await using</c> scope. The registration is restored
/// automatically after a reconnect.
/// </remarks>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "The small semaphore remains valid for concurrent callers racing terminal disposal.")]
public sealed class RpcWorkerRegistration : IAsyncDisposable
{
    readonly Func<CancellationToken, ValueTask> _unregister;
    readonly SemaphoreSlim _unregisterGate = new(1, 1);
    int _unregistered;

    /// <summary>
    /// Initializes the registration.
    /// </summary>
    /// <param name="pattern">Route or pattern this worker serves.</param>
    /// <param name="unregister">Callback that withdraws the registration from the broker.</param>
    public RpcWorkerRegistration(
        string pattern,
        Func<CancellationToken, ValueTask> unregister)
    {
        Pattern = pattern;
        _unregister = unregister;
    }

    /// <summary>The route or pattern this worker serves.</summary>
    public string Pattern { get; }

    /// <summary>
    /// Withdraws the worker registration. Safe to call more than once.
    /// </summary>
    /// <param name="ct">Cancellation token bounding the unregister request.</param>
    /// <returns>A task that completes once the broker accepts the withdrawal.</returns>
    public ValueTask UnregisterAsync(CancellationToken ct = default) => UnregisterCoreAsync(ct);

    /// <summary>
    /// Unregisters on a best-effort basis, bounded to five seconds, and swallows failures.
    /// </summary>
    /// <returns>A task that completes once cleanup finishes.</returns>
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

    async ValueTask UnregisterCoreAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _unregistered) != 0)
        {
            return;
        }

        await _unregisterGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _unregistered) != 0)
            {
                return;
            }

            await _unregister(ct).ConfigureAwait(false);
            Volatile.Write(ref _unregistered, 1);
        }
        finally
        {
            _unregisterGate.Release();
        }
    }
}
