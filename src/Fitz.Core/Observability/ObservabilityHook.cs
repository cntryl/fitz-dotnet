namespace Cntryl.Fitz.Observability;

/// <summary>
/// Runs caller-supplied observability hooks (loggers, tracers, meters, lifecycle callbacks)
/// so that a failing hook cannot change client behaviour.
/// </summary>
/// <remarks>
/// Hooks take their inputs as explicit state, so call sites can pass <see langword="static"/>
/// lambdas and allocate no closure on hot paths.
/// </remarks>
static class ObservabilityHook
{
    /// <summary>Invokes <paramref name="hook"/>, absorbing any recoverable failure.</summary>
    /// <typeparam name="TState">The hook's inputs.</typeparam>
    /// <param name="state">The hook's inputs.</param>
    /// <param name="hook">The caller-supplied hook.</param>
    internal static void Invoke<TState>(TState state, Action<TState> hook)
    {
        try
        {
            hook(state);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            // Observability must never alter client behaviour.
        }
    }

    /// <summary>Invokes <paramref name="hook"/>, returning the default value if it fails.</summary>
    /// <typeparam name="TState">The hook's inputs.</typeparam>
    /// <typeparam name="TResult">The hook's result.</typeparam>
    /// <param name="state">The hook's inputs.</param>
    /// <param name="hook">The caller-supplied hook.</param>
    /// <returns>The hook's result, or <see langword="default"/> if it threw.</returns>
    internal static TResult? Invoke<TState, TResult>(TState state, Func<TState, TResult> hook)
    {
        try
        {
            return hook(state);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            return default;
        }
    }
}
