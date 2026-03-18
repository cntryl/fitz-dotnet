namespace Cntryl.Fitz.Abstractions.Domains.Queue;

public sealed record QueueItem(string Route, ulong Id, ulong Token, byte[] Body);