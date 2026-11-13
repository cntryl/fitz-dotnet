namespace Cntryl.Fitz;

/// <summary>
/// An active RPC worker registration. Dispose to withdraw it.
/// </summary>
/// <remarks>
/// Unregistering twice is safe. Disposal is bounded best-effort and never replaces an
/// exception already leaving an <c>await using</c> scope. The registration is restored
/// automatically after a reconnect.
/// </remarks>
public sealed class RpcWorkerRegistration : IAsyncDisposable
{
    static readonly TimeSpan CleanupBudget = TimeSpan.FromSeconds(5);

    readonly Func<CancellationToken, ValueTask> _unregister;
    readonly SemaphoreSlim _unregisterGate = new(1, 1);
    readonly CancellationTokenSource _closing = new();
    readonly CancellationToken _closingToken;
    bool _unregistered;
    bool _disposed;

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
        _closingToken = _closing.Token;
    }

    /// <summary>The route or pattern this worker serves.</summary>
    public string Pattern { get; }

    /// <summary>
    /// Withdraws the worker registration. Safe to call more than once.
    /// </summary>
    /// <param name="ct">Cancellation token bounding the unregister request.</param>
    /// <returns>A task that completes once the broker accepts the withdrawal.</returns>
    /// <remarks>
    /// Once the registration is disposed this returns without contacting the broker, because
    /// disposal already unregistered.
    /// </remarks>
    public async ValueTask UnregisterAsync(CancellationToken ct = default)
    {
        if (Volatile.Read(ref _unregistered))
            return;

        if (!await _unregisterGate.TryEnterAsync(_closingToken, ct).ConfigureAwait(false))
            return;

        try
        {
            await UnregisterOnceAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _unregisterGate.Release();
        }
    }

    /// <summary>
    /// Unregisters on a best-effort basis, bounded to five seconds, and swallows failures.
    /// </summary>
    /// <returns>A task that completes once cleanup finishes.</returns>
    /// <remarks>
    /// Waits for an in-progress <see cref="UnregisterAsync"/> to finish first. Calls that are
    /// still queued, or that start afterwards, return without contacting the broker.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, true))
            return;

        await _closing.CancelAsync().ConfigureAwait(false);
        await _unregisterGate.DrainAndDisposeAsync().ConfigureAwait(false);
        _closing.Dispose();
        try
        {
            using var cleanupDeadline = new CancellationTokenSource(CleanupBudget);
            await UnregisterOnceAsync(cleanupDeadline.Token).ConfigureAwait(false);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            // Best-effort cleanup must not replace an exception leaving an await-using scope.
        }
    }

    // Callers hold the gate, or have drained it during disposal.
    async ValueTask UnregisterOnceAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _unregistered))
            return;

        await _unregister(ct).ConfigureAwait(false);
        Volatile.Write(ref _unregistered, true);
    }
}
