namespace Cntryl.Fitz.DependencyInjection;

/// <summary>Selects how the hosted lifecycle of a registered Fitz client connects at host startup.</summary>
public enum FitzClientStartupMode
{
    /// <summary>
    /// Call <c>ConnectWhenReadyAsync</c>: retry transport failures until the deadline, so a host
    /// that starts before its broker does not fail. An authentication rejection still fails
    /// startup immediately. This is the default.
    /// </summary>
    ConnectWhenReady = 0,

    /// <summary>
    /// Call <c>ConnectAsync</c> once: host startup fails if the broker cannot be reached at that
    /// moment. Choose this to fail fast.
    /// </summary>
    Connect = 1,

    /// <summary>
    /// Do not connect during startup. The application connects the client itself; the hosted
    /// lifecycle still closes it at shutdown.
    /// </summary>
    Manual = 2,
}

/// <summary>Configures the hosted lifecycle that <c>AddFitzClient</c> registers.</summary>
public sealed record FitzClientHostingOptions
{
    /// <summary>
    /// Gets how the client connects during host startup. Defaults to
    /// <see cref="FitzClientStartupMode.ConnectWhenReady"/>.
    /// </summary>
    public FitzClientStartupMode Startup { get; init; } = FitzClientStartupMode.ConnectWhenReady;

    /// <summary>
    /// Gets the timeout and backoff used by <see cref="FitzClientStartupMode.ConnectWhenReady"/>.
    /// When <see langword="null"/>, the client's own defaults apply: its configured request timeout
    /// bounds the total wait.
    /// </summary>
    public ConnectWhenReadyOptions? ConnectWhenReady { get; init; }
}
