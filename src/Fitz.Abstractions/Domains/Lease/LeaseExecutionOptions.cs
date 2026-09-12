namespace Cntryl.Fitz;

/// <summary>
/// Controls how <c>ILeaseClient.WithLeaseAsync</c> behaves when a lease is already held.
/// </summary>
public sealed class LeaseExecutionOptions
{
    /// <summary>
    /// Whether to wait for a contended lease instead of failing immediately.
    /// Defaults to <see langword="false"/>.
    /// </summary>
    public bool WaitForAvailability { get; init; }

    /// <summary>
    /// How long to wait when <see cref="WaitForAvailability"/> is enabled.
    /// </summary>
    public TimeSpan Wait { get; init; } = TimeSpan.FromSeconds(30);
}
