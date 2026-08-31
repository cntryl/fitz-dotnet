namespace Cntryl.Fitz.Abstractions.Domains.Lease;

/// <summary>Tuning knobs for <see cref="ILeaseClient.ObserveAsync"/>.</summary>
public sealed record LeaseObserveOptions
{
    /// <summary>Default backstop reconciliation interval (60 seconds).</summary>
    public static readonly TimeSpan DefaultReconciliationInterval = TimeSpan.FromSeconds(60);

    /// <summary>Default jitter ratio applied to the reconciliation interval (±20%).</summary>
    public const double DefaultReconciliationJitterRatio = 0.2;

    /// <summary>
    /// How often the observer performs a full LIST-based reconciliation as a backstop against a
    /// missed or dropped LEASE_NOTIFY. Defaults to 60 seconds.
    /// </summary>
    public TimeSpan ReconciliationInterval { get; init; } = DefaultReconciliationInterval;

    /// <summary>
    /// Randomized jitter applied to <see cref="ReconciliationInterval"/> on each cycle, expressed
    /// as a fraction of the interval (0.2 means ±20%), so a fleet of observers does not reconcile
    /// in lockstep. Defaults to 0.2.
    /// </summary>
    public double ReconciliationJitterRatio { get; init; } = DefaultReconciliationJitterRatio;
}
