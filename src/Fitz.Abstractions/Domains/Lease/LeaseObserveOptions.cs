namespace Cntryl.Fitz.Abstractions.Domains.Lease;

/// <summary>Tuning knobs for <see cref="ILeaseClient.ObserveAsync"/>.</summary>
public sealed record LeaseObserveOptions
{
    /// <summary>Default backstop reconciliation interval (60 seconds).</summary>
    public static readonly TimeSpan DefaultReconciliationInterval = TimeSpan.FromSeconds(60);

    /// <summary>Default jitter ratio applied to the reconciliation interval (±20%).</summary>
    public const double DefaultReconciliationJitterRatio = 0.2;

    /// <summary>Default bounded capacity of the observer update stream.</summary>
    public const int DefaultUpdateBufferCapacity = 256;

    /// <summary>Default deadline for one complete paginated inventory LIST pass.</summary>
    public static readonly TimeSpan DefaultListTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How often the observer performs a full LIST-based reconciliation as a backstop against a
    /// missed or dropped LEASE_NOTIFY. Defaults to 60 seconds. For a known workload, use
    /// <c>clamp(shortest expected lease TTL / 2, 5 seconds, 60 seconds)</c>; this targets two
    /// backstop passes during the shortest lease lifetime without polling faster than
    /// one bounded full LIST every five seconds per observer.
    /// </summary>
    public TimeSpan ReconciliationInterval { get; init; } = DefaultReconciliationInterval;

    /// <summary>
    /// Randomized jitter applied to <see cref="ReconciliationInterval"/> on each cycle, expressed
    /// as a fraction of the interval (0.2 means ±20%), so a fleet of observers does not reconcile
    /// in lockstep. Must be in the range [0, 1). Defaults to 0.2.
    /// </summary>
    public double ReconciliationJitterRatio { get; init; } = DefaultReconciliationJitterRatio;

    /// <summary>
    /// Maximum number of pending entries retained by <see cref="ILeaseInventoryObserver.Updates"/>.
    /// Once full, the oldest pending update is dropped because <see cref="ILeaseInventoryObserver.View"/>
    /// remains the authoritative current snapshot. Defaults to 256.
    /// </summary>
    public int UpdateBufferCapacity { get; init; } = DefaultUpdateBufferCapacity;

    /// <summary>
    /// Maximum duration of one complete paginated LIST pass. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan ListTimeout { get; init; } = DefaultListTimeout;
}
