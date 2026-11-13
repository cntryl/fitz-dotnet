using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Connection;

/// <summary>
/// Doubling delays with ±20% jitter, capped at a maximum. Shared by request retry and
/// reconnect so both back off the same way.
/// </summary>
struct ExponentialBackoff
{
    const double JitterRatio = 0.2;

    readonly TimeSpan _maximum;
    TimeSpan _current;

    /// <summary>Starts at <paramref name="initial"/>.</summary>
    /// <param name="initial">The first delay.</param>
    /// <param name="maximum">The cap, or <see cref="Timeout.InfiniteTimeSpan"/> for none.</param>
    internal ExponentialBackoff(TimeSpan initial, TimeSpan maximum)
    {
        _current = initial;
        _maximum = maximum;
    }

    /// <summary>Returns the next jittered delay and doubles the one after it.</summary>
    /// <returns>The delay to wait before the next attempt.</returns>
    internal TimeSpan Next()
    {
        var delay = Cap(Jitter.Apply(_current, JitterRatio));
        _current = Cap(Double(_current));
        return delay;
    }

    readonly TimeSpan Cap(TimeSpan delay) =>
        _maximum == Timeout.InfiniteTimeSpan || delay <= _maximum ? delay : _maximum;

    static TimeSpan Double(TimeSpan delay)
    {
        if (delay == Timeout.InfiniteTimeSpan)
            return delay;

        return delay.Ticks > TimeSpan.MaxValue.Ticks / 2
            ? TimeSpan.MaxValue
            : TimeSpan.FromTicks(delay.Ticks * 2);
    }
}
