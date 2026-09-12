namespace Cntryl.Fitz;

/// <summary>
/// A byte-framed connection to a Fitz broker.
/// </summary>
/// <remarks>
/// Supply a custom implementation through <c>ClientConfig.TransportFactory</c>. Received
/// frames are pooled: dispose each <see cref="PooledFrame"/> once decoded.
/// </remarks>
public interface ITransport : IAsyncDisposable
{
    /// <summary>
    /// Stable diagnostic label for this transport, used for the <c>network.transport</c>
    /// span tag and the transport field of lifecycle events.
    /// </summary>
    /// <remarks>
    /// Return a compile-time constant. The default deliberately avoids
    /// <c>GetType().Name</c> so the client performs no runtime type inspection; override
    /// it to identify a custom transport in telemetry.
    /// </remarks>
    string TransportName => "custom";

    /// <summary>The endpoint this transport connects to.</summary>
    Uri Url { get; }
    /// <summary>Opens the connection.</summary>
    /// <param name="ct">Cancellation token for the attempt.</param>
    /// <returns>A task that completes once the connection is open.</returns>
    Task ConnectAsync(CancellationToken ct = default);
    /// <summary>Sends one encoded frame.</summary>
    /// <param name="data">The encoded frame. Must remain valid until the task completes.</param>
    /// <param name="ct">Cancellation token for the send.</param>
    /// <returns>A task that completes once the frame is written.</returns>
    Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default);
    /// <summary>Receives the next frame.</summary>
    /// <param name="ct">Cancellation token for the receive.</param>
    /// <returns>
    /// The next frame, or a frame whose <see cref="PooledFrame.IsClosed"/> is
    /// <see langword="true"/> when the peer closed. Dispose it once decoded.
    /// </returns>
    ValueTask<PooledFrame> ReceiveAsync(CancellationToken ct = default);
    /// <summary>Closes the connection gracefully.</summary>
    /// <param name="ct">Cancellation token bounding the close.</param>
    /// <returns>A task that completes once the connection is closed.</returns>
    Task CloseAsync(CancellationToken ct = default);
}
