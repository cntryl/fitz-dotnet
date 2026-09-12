namespace Cntryl.Fitz;

/// <summary>
/// WebSocket-specific transport settings.
/// </summary>
/// <param name="Headers">Additional headers sent with the WebSocket upgrade request.</param>
public sealed record WebSocketOptions(
    IReadOnlyDictionary<string, string>? Headers = null
);
