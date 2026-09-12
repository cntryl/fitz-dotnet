namespace Cntryl.Fitz;

/// <summary>
/// Bounds the retry loop used by <c>IClient.ConnectWhenReadyAsync</c>.
/// </summary>
/// <param name="Timeout">
/// Total time to keep retrying before giving up. Defaults to the client's configured timeout.
/// </param>
/// <param name="Backoff">Delay before the first retry. Subsequent delays grow from it.</param>
/// <param name="MaxBackoff">Ceiling applied to the growing retry delay.</param>
public sealed record ConnectWhenReadyOptions(
    TimeSpan? Timeout = null,
    TimeSpan? Backoff = null,
    TimeSpan? MaxBackoff = null
);
