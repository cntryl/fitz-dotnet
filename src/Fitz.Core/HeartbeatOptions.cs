namespace Cntryl.Fitz;

/// <summary>
/// Controls transport-native keepalive.
/// </summary>
/// <param name="Enabled">Whether keepalive is active. Enabled by default.</param>
/// <param name="Interval">How often to send a keepalive probe.</param>
/// <param name="Timeout">
/// How long to wait for a response before treating the connection as lost. For WebSocket
/// this configures native PING/PONG timeout detection.
/// </param>
/// <remarks>
/// This is transport keepalive, not an application heartbeat: the Fitz protocol has no
/// application-level heartbeat frame. TCP uses socket keepalive.
/// </remarks>
public sealed record HeartbeatOptions(
    bool Enabled = true,
    TimeSpan? Interval = null,
    TimeSpan? Timeout = null
);
