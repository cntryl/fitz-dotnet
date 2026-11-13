namespace Cntryl.Fitz.Connection;

readonly record struct SessionResponse(ReadOnlyMemory<byte> Payload, CancellationToken Closed);
