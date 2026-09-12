namespace Cntryl.Fitz;

/// <summary>
/// Converts between <see cref="TimeSpan"/> and the integer durations the Fitz wire carries.
/// </summary>
/// <remarks>
/// The public API speaks <see cref="TimeSpan"/> because that is what a .NET caller expects; the
/// protocol speaks whole seconds. Precision the wire cannot represent is rejected rather than
/// rounded away: silently turning a 1500 ms lease into one second would produce a shorter hold
/// than the caller asked for, which is exactly the kind of difference that only shows up under
/// contention.
/// </remarks>
static class WireDuration
{
    /// <summary>Largest whole second count <see cref="TimeSpan"/> can represent.</summary>
    const ulong MaxWholeSeconds = (ulong)(long.MaxValue / TimeSpan.TicksPerSecond);

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

    /// <summary>
    /// Converts a duration read off the wire, reporting rather than throwing when the value
    /// is too large for <see cref="TimeSpan"/>. A malformed response is the caller's to
    /// report with its own domain error code, not ours to surface as an
    /// <see cref="OverflowException"/> from inside a decode loop.
    /// </summary>
    internal static bool TryFromSeconds(ulong seconds, out TimeSpan value)
    {
        if (seconds > MaxWholeSeconds)
        {
            value = default;
            return false;
        }

        value = TimeSpan.FromSeconds(seconds);
        return true;
    }

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
