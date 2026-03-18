namespace Cntryl.Fitz.Abstractions.Domains.Rpc;

/// <summary>
/// RPC request received by a registered worker.
/// Represents an incoming RPC call that the worker should handle and respond to.
/// </summary>
public sealed record RpcRequest(string Route, ReadOnlyMemory<byte> Body);

/// <summary>
/// Writer for streaming RPC responses.
/// Worker uses this to send response frames back to the caller.
/// </summary>
public interface IRpcResponseWriter
{
    /// <summary>
    /// Sends a response frame. Set isEnd=true to finalize the RPC response stream.
    /// </summary>
    ValueTask SendAsync(ReadOnlyMemory<byte> body, bool isEnd = false, CancellationToken ct = default);
}

/// <summary>
/// RPC response frame received by the caller.
/// Multiple frames can be received for streaming RPC calls.
/// </summary>
public sealed record RpcResponseFrame(ReadOnlyMemory<byte> Body, ulong Sequence);
