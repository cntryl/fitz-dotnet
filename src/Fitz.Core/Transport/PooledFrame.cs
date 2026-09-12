using System.Buffers;

namespace Cntryl.Fitz;

/// <summary>
/// A received frame backed by a pooled buffer.
/// </summary>
/// <remarks>
/// Dispose returns the buffer to the pool. <see cref="Memory"/> is invalid after disposal,
/// so copy anything you need to outlive the frame.
/// </remarks>
public sealed class PooledFrame : IDisposable
{
    byte[]? _buffer;
    readonly bool _isClosed;

    PooledFrame(byte[]? buffer, int length, bool isClosed)
    {
        _buffer = buffer;
        Length = length;
        _isClosed = isClosed;
    }

    /// <summary>Number of valid bytes in <see cref="Memory"/>.</summary>
    public int Length { get; }

    /// <summary>Whether this frame signals that the peer closed the connection.</summary>
    public bool IsClosed => _isClosed;

    /// <summary>The frame bytes. Valid only until the frame is disposed.</summary>
    public ReadOnlyMemory<byte> Memory
    {
        get
        {
            var buffer = _buffer ?? throw new ObjectDisposedException(nameof(PooledFrame));
            return buffer.AsMemory(0, Length);
        }
    }

    /// <summary>A frame with no bytes that does not signal closure.</summary>
    public static PooledFrame Empty => new(Array.Empty<byte>(), 0, isClosed: false);

    /// <summary>A frame signalling that the peer closed the connection.</summary>
    public static PooledFrame Closed => new(Array.Empty<byte>(), 0, isClosed: true);

    /// <summary>Wraps a buffer already rented from the shared pool.</summary>
    /// <param name="buffer">Rented buffer. Ownership transfers to the returned frame.</param>
    /// <param name="length">Number of valid bytes in the buffer.</param>
    /// <returns>A frame that returns the buffer to the pool when disposed.</returns>
    public static PooledFrame FromRentedBuffer(byte[] buffer, int length)
    {
        ArgumentNullException.ThrowIfNull(buffer);

        if (length < 0 || length > buffer.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(length));
        }

        return new PooledFrame(buffer, length, isClosed: false);
    }

    /// <summary>Returns the pooled buffer. Safe to call more than once.</summary>
    public void Dispose()
    {
        var buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer is null || ReferenceEquals(buffer, Array.Empty<byte>()))
        {
            return;
        }

        // Clear only the region that held frame bytes. Zeroing the whole rented array costs
        // time proportional to the rent size (16 KB is typical) rather than the frame size.
        buffer.AsSpan(0, Length).Clear();
        ArrayPool<byte>.Shared.Return(buffer);
    }
}
