namespace Cntryl.Fitz.Abstractions.Domains.Lease;

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
    /// How long to wait when <see cref="WaitForAvailability"/> is enabled, in seconds.
    /// </summary>
    public uint WaitSeconds { get; init; } = 30;
}
