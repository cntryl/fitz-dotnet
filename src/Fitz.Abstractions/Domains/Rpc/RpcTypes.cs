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
    /// <summary>Dispatch or cancellation could not be confirmed.</summary>
    Unconfirmed,
    /// <summary>The connection closed while cancellation was in progress.</summary>
    ConnectionClosed,
}

/// <summary>A lazy, single-use streaming RPC response sequence and its remote cancellation result.</summary>
public sealed class RpcCall : IAsyncEnumerable<RpcResponseFrame>
{
    readonly IAsyncEnumerable<RpcResponseFrame> _frames;

    readonly TaskCompletionSource<Task<RpcCancellationOutcome>> _outcome = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly Task<RpcCancellationOutcome> _dispatchedOutcome;
    bool _claimed;

    /// <summary>Creates a lazy RPC call handle.</summary>
    /// <param name="frames">Response sequence.</param>
    /// <param name="cancellation">Task that resolves to the broker cancellation result.</param>
    public RpcCall(IAsyncEnumerable<RpcResponseFrame> frames, Task<RpcCancellationOutcome> cancellation)
    {
        _frames = frames ?? throw new ArgumentNullException(nameof(frames));
        _dispatchedOutcome = cancellation ?? throw new ArgumentNullException(nameof(cancellation));
        Cancellation = _outcome.Task.Unwrap();
    }

    /// <summary>Completes with the broker's result after enumeration is cancelled or ends.</summary>
    public Task<RpcCancellationOutcome> Cancellation { get; }

    /// <inheritdoc />
    public IAsyncEnumerator<RpcResponseFrame> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _claimed, true))
            throw new InvalidOperationException("An RPC call can only be enumerated once.");

        return new Enumerator(this, cancellationToken);
    }

    sealed class Enumerator(RpcCall call, CancellationToken ct) : IAsyncEnumerator<RpcResponseFrame>
    {
        IAsyncEnumerator<RpcResponseFrame>? _inner;
        bool _disposed;

        public RpcResponseFrame Current => _inner!.Current;

        public ValueTask<bool> MoveNextAsync()
        {
            if (_disposed)
                return ValueTask.FromResult(false);


            if (_inner is null)
            {
                _inner = call._frames.GetAsyncEnumerator(ct);
                call._outcome.SetResult(call._dispatchedOutcome);
            }

            return _inner.MoveNextAsync();
        }

        public ValueTask DisposeAsync()
        {
            if (_disposed)
                return ValueTask.CompletedTask;

            _disposed = true;

            if (_inner is not null)
                return _inner.DisposeAsync();

            call._outcome.TrySetResult(Task.FromResult(RpcCancellationOutcome.RequestNotSent));
            return ValueTask.CompletedTask;
        }
    }
}

/// <summary>
/// RPC request received by a registered worker.
/// Represents an incoming RPC call that the worker should handle and respond to.
/// </summary>
public sealed record RpcRequest(string Route, ReadOnlyMemory<byte> Body)
{
    readonly TimeSpan? _initialRemainingTime;
    readonly TimeProvider _time = TimeProvider.System;
    readonly long _budgetStartedAt;

    /// <summary>Creates a request with an optional remaining end-to-end budget.</summary>
    /// <param name="route">Concrete RPC route.</param>
    /// <param name="body">Opaque request payload.</param>
    /// <param name="remainingTime">Remaining budget, or null when no budget was supplied.</param>
    /// <param name="timeProvider">The clock that counts the budget down; the system clock by default.</param>
    public RpcRequest(string route, ReadOnlyMemory<byte> body, TimeSpan? remainingTime, TimeProvider? timeProvider = null)
        : this(route, body)
    {
        _initialRemainingTime = remainingTime;
        _time = timeProvider ?? TimeProvider.System;
        _budgetStartedAt = _time.GetTimestamp();
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

            var remaining = budget - _time.GetElapsedTime(_budgetStartedAt);
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
