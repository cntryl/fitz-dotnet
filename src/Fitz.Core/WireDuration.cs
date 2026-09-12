namespace Cntryl.Fitz;

/// <summary>
/// Converts between <see cref="TimeSpan"/> and the integer durations the Fitz wire carries.
/// </summary>
/// <remarks>
/// The public API speaks <see cref="TimeSpan"/> because that is what a .NET caller expects; the
/// protocol speaks whole seconds, or milliseconds for queue delays. Precision the wire cannot
/// represent is rejected rather than rounded away: silently turning a 1500 ms lease into one
/// second would produce a shorter hold than the caller asked for, which is exactly the kind of
/// difference that only shows up under contention.
/// </remarks>
static class WireDuration
{
    internal static ulong ToSeconds(TimeSpan value, string paramName)
    {
        ThrowIfNegative(value, paramName);
        ThrowIfFractionalSeconds(value, paramName);

        var seconds = value.TotalSeconds;
        if (seconds > ulong.MaxValue)
        {
            throw new ArgumentOutOfRangeException(paramName, value, "Duration exceeds the Fitz wire range.");
        }

        return (ulong)seconds;
    }

    internal static uint ToSecondsUInt32(TimeSpan value, string paramName)
    {
        ThrowIfNegative(value, paramName);
        ThrowIfFractionalSeconds(value, paramName);

        var seconds = value.TotalSeconds;
        if (seconds > uint.MaxValue)
        {
            throw new ArgumentOutOfRangeException(paramName, value, "Duration exceeds the Fitz wire range.");
        }

        return (uint)seconds;
    }

    internal static int ToMilliseconds(TimeSpan value, string paramName)
    {
        ThrowIfNegative(value, paramName);

        var milliseconds = value.TotalMilliseconds;
        if (milliseconds != Math.Floor(milliseconds))
        {
            throw new ArgumentOutOfRangeException(
                paramName, value, "Duration must be a whole number of milliseconds; the Fitz wire carries no finer unit.");
        }

        if (milliseconds > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(paramName, value, "Duration exceeds the Fitz wire range.");
        }

        return (int)milliseconds;
    }

    internal static TimeSpan FromSeconds(ulong seconds) => TimeSpan.FromSeconds(seconds);

    static void ThrowIfNegative(TimeSpan value, string paramName)
    {
        if (value < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(paramName, value, "Duration cannot be negative.");
        }
    }

    static void ThrowIfFractionalSeconds(TimeSpan value, string paramName)
    {
        if (value.Ticks % TimeSpan.TicksPerSecond != 0)
        {
            throw new ArgumentOutOfRangeException(
                paramName, value, "Duration must be a whole number of seconds; the Fitz wire carries no finer unit here.");
        }
    }
}
