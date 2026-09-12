namespace Cntryl.Fitz;

/// <summary>
/// Controls automatic reconnection after the connection is lost.
/// </summary>
/// <param name="Enabled">Whether to reconnect automatically. Enabled by default.</param>
/// <param name="MaxAttempts">Maximum reconnect attempts before giving up.</param>
/// <param name="Backoff">Delay before the first attempt. Subsequent delays grow from it.</param>
/// <param name="MaxBackoff">Ceiling applied to the growing delay.</param>
/// <remarks>
/// Active subscriptions and worker registrations are restored after a successful reconnect.
/// An authentication rejection is authoritative and ends the loop rather than retrying.
/// </remarks>
public sealed record ReconnectOptions(
    bool Enabled = true,
    int MaxAttempts = int.MaxValue,
    TimeSpan? Backoff = null,
    TimeSpan? MaxBackoff = null
);
