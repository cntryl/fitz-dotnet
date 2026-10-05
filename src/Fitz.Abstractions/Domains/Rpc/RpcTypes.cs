namespace Cntryl.Fitz;

/// <summary>Reports how the broker handled a best-effort RPC cancellation request.</summary>
public enum RpcCancellationOutcome
{
    /// <summary>No cancellation was requested before the call completed.</summary>
    NotRequested,
    /// <summary>The request was not sent to the broker.</summary>
    RequestNotSent,
    /// <summary>The broker did not advertise RPC cancellation support.</summary>
    Unsupported,
    /// <summary>The broker removed the request from its worker queue.</summary>
    QueuedRemoved,
    /// <summary>The broker routed a cancellation signal to the worker.</summary>
    Forwarded,
    /// <summary>The selected worker did not negotiate cancellation support.</summary>
    WorkerUnsupported,
    /// <summary>The RPC already reached a terminal state.</summary>
    AlreadyTerminal,
    /// <summary>The request was unknown or was not owned by this caller.</summary>
    UnknownOrUnauthorized,
    /// <summary>The broker could not forward the cancellation signal.</summary>
    ForwardingFailed,
    /// <summary>No broker result arrived before the local confirmation timeout.</summary>
    Unconfirmed,
    /// <summary>The connection closed while cancellation was in progress.</summary>
    ConnectionClosed,
}

/// <summary>A streaming RPC response sequence and its remote cancellation result.</summary>
public sealed class RpcCall : IAsyncEnumerable<RpcResponseFrame>
{
    readonly IAsyncEnumerable<RpcResponseFrame> _frames;

    /// <summary>Creates a lazy RPC call handle.</summary>
    /// <param name="frames">Response sequence.</param>
    /// <param name="cancellation">Task that resolves to the broker cancellation result.</param>
    public RpcCall(IAsyncEnumerable<RpcResponseFrame> frames, Task<RpcCancellationOutcome> cancellation)
    {
        _frames = frames ?? throw new ArgumentNullException(nameof(frames));
        Cancellation = cancellation ?? throw new ArgumentNullException(nameof(cancellation));
    }

    /// <summary>Completes with the broker's result after enumeration is cancelled or ends.</summary>
    public Task<RpcCancellationOutcome> Cancellation { get; }

    /// <inheritdoc />
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1725:Parameter names should match base declaration", Justification = "Fitz public cancellation token parameters use the established ct name.")]
    public IAsyncEnumerator<RpcResponseFrame> GetAsyncEnumerator(CancellationToken ct = default) =>
        _frames.GetAsyncEnumerator(ct);
}

/// <summary>
/// RPC request received by a registered worker.
/// Represents an incoming RPC call that the worker should handle and respond to.
/// </summary>
public sealed record RpcRequest(string Route, ReadOnlyMemory<byte> Body)
{
    readonly TimeSpan? _initialRemainingTime;
    readonly long _budgetStartedAt;

    /// <summary>Creates a request with an optional remaining end-to-end budget.</summary>
    /// <param name="route">Concrete RPC route.</param>
    /// <param name="body">Opaque request payload.</param>
    /// <param name="remainingTime">Remaining budget, or null when no budget was supplied.</param>
    public RpcRequest(string route, ReadOnlyMemory<byte> body, TimeSpan? remainingTime)
        : this(route, body)
    {
        _initialRemainingTime = remainingTime;
        _budgetStartedAt = System.Diagnostics.Stopwatch.GetTimestamp();
    }

    /// <summary>Current remaining end-to-end budget, if the caller supplied one.</summary>
    public TimeSpan? RemainingTime
    {
        get
        {
            if (_initialRemainingTime is not { } budget)
            {
                return null;
            }

            var remaining = budget - System.Diagnostics.Stopwatch.GetElapsedTime(_budgetStartedAt);
            return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
        }
    }
}

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
