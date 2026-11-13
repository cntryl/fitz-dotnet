namespace Cntryl.Fitz;

/// <summary>
/// Decides which exceptions an isolation boundary may absorb.
/// </summary>
/// <remarks>
/// Use it only where a catch deliberately isolates code the boundary cannot vouch for, such as
/// a caller-supplied callback or best-effort teardown, as
/// <c>catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))</c>. Everywhere else,
/// catch the concrete exception types the guarded call can throw.
/// </remarks>
static class ExceptionPolicy
{
    /// <summary>
    /// Whether the process can continue safely after <paramref name="error"/>. Exceptions that
    /// signal a corrupted or exhausted runtime are never recoverable and must propagate.
    /// </summary>
    /// <param name="error">The exception being considered.</param>
    /// <returns><see langword="false"/> for fatal runtime failures.</returns>
    internal static bool IsRecoverable(Exception error) => error is not (
        OutOfMemoryException
        or InsufficientExecutionStackException
        or StackOverflowException
        or AccessViolationException);
}
