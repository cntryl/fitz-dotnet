using System.Security.Cryptography;

namespace Cntryl.Fitz.Runtime;

/// <summary>
/// Randomizes retry and polling delays so that many clients do not act in lockstep.
/// </summary>
/// <remarks>
/// Uses <see cref="RandomNumberGenerator"/>: the values need no secrecy, but the shared
/// generator is thread-safe and its cost is negligible at retry and polling rates.
/// </remarks>
static class Jitter
{
    const int Resolution = 1_000_000;

    /// <summary>
    /// Scales <paramref name="delay"/> by a uniformly random factor in
    /// [1 - <paramref name="ratio"/>, 1 + <paramref name="ratio"/>].
    /// </summary>
    /// <param name="delay">The nominal delay. Infinite and non-positive delays are returned unchanged.</param>
    /// <param name="ratio">The maximum relative deviation, in [0, 1).</param>
    /// <returns>The randomized delay, saturating at <see cref="TimeSpan.MaxValue"/>.</returns>
    internal static TimeSpan Apply(TimeSpan delay, double ratio)
    {
        if (delay <= TimeSpan.Zero || delay == Timeout.InfiniteTimeSpan || ratio <= 0)
            return delay;

        var unit = RandomNumberGenerator.GetInt32(Resolution + 1) / (double)Resolution;
        var factor = 1 + (((unit * 2) - 1) * ratio);
        return delay.Ticks > TimeSpan.MaxValue.Ticks / factor
            ? TimeSpan.MaxValue
            : TimeSpan.FromTicks((long)(delay.Ticks * factor));
    }
}
