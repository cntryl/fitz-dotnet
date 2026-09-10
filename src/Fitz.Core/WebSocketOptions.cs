namespace Cntryl.Fitz;

public sealed record WebSocketOptions(
    IReadOnlyDictionary<string, string>? Headers = null
);
