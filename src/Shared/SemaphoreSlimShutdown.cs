namespace Cntryl.Fitz;

/// <summary>
/// Ordered shutdown for a <see cref="SemaphoreSlim"/> owned by a disposable type.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="SemaphoreSlim.Dispose()"/> is not safe while the semaphore is in use: it does not
/// wake queued <see cref="SemaphoreSlim.WaitAsync(CancellationToken)"/> callers, and a holder that
/// releases afterwards throws. An owner therefore shuts its gate down in order:
/// </para>
/// <list type="number">
/// <item>Cancel the owner's closing token. Queued and later entries observe it and give up.</item>
/// <item>Call <see cref="DrainAndDisposeAsync"/>, which waits for the current holders to release
/// every permit, keeps them, and disposes the semaphore.</item>
/// </list>
/// <para>
/// Every entry must go through <see cref="TryEnterAsync"/> or <see cref="EnterAsync"/> with the
/// owner's closing token, so that no caller can queue behind the drain and wait forever.
/// </para>
/// </remarks>
static class SemaphoreSlimShutdown
{
    /// <summary>
    /// Enters the gate unless its owner is closing.
    /// </summary>
    /// <param name="gate">The owner's gate.</param>
    /// <param name="closing">Cancelled when the owner begins shutting down.</param>
    /// <param name="ct">The caller's cancellation token.</param>
    /// <returns>
    /// <see langword="true"/> when the caller now holds a permit and must release it;
    /// <see langword="false"/> when the owner is closing and no permit was taken.
    /// </returns>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> was cancelled.</exception>
    internal static async ValueTask<bool> TryEnterAsync(
        this SemaphoreSlim gate,
        CancellationToken closing,
        CancellationToken ct)
    {
        if (closing.IsCancellationRequested)
            return false;

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(closing, ct);
        try
        {
            await gate.WaitAsync(linked.Token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (closing.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            return false;
        }
        catch (ObjectDisposedException) when (closing.IsCancellationRequested)
        {
            return false;
        }
    }

    /// <summary>
    /// Enters the gate, or throws <see cref="ObjectDisposedException"/> when its owner is closing.
    /// </summary>
    /// <param name="gate">The owner's gate.</param>
    /// <param name="owner">The owning instance, named in the exception.</param>
    /// <param name="closing">Cancelled when the owner begins shutting down.</param>
    /// <param name="ct">The caller's cancellation token.</param>
    /// <returns>A task that completes once the caller holds a permit, which it must release.</returns>
    /// <exception cref="ObjectDisposedException">The owner is closing.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> was cancelled.</exception>
    internal static async ValueTask EnterAsync(
        this SemaphoreSlim gate,
        object owner,
        CancellationToken closing,
        CancellationToken ct)
    {
        var entered = await gate.TryEnterAsync(closing, ct).ConfigureAwait(false);
        ObjectDisposedException.ThrowIf(!entered, owner);
    }

    /// <summary>
    /// Waits for current holders to release all <paramref name="permits"/>, then disposes the gate.
    /// </summary>
    /// <param name="gate">The owner's gate.</param>
    /// <param name="permits">The gate's maximum count.</param>
    /// <returns>A task that completes once the gate is disposed.</returns>
    /// <remarks>
    /// Cancel the owner's closing token first, so that no new caller can take a permit.
    /// </remarks>
    internal static async ValueTask DrainAndDisposeAsync(this SemaphoreSlim gate, int permits = 1)
    {
        for (var held = 0; held < permits; held++)
            await gate.WaitAsync().ConfigureAwait(false);

        gate.Dispose();
    }
}
