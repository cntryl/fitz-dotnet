using System.Buffers;

namespace Cntryl.Fitz.Transport;

public sealed class PooledFrame : IDisposable
{
    private byte[]? _buffer;
    private readonly bool _isClosed;

    private PooledFrame(byte[]? buffer, int length, bool isClosed)
    {
        _buffer = buffer;
        Length = length;
        _isClosed = isClosed;
    }

    public int Length { get; }

    public bool IsClosed => _isClosed;

    public ReadOnlyMemory<byte> Memory => _buffer is null ? ReadOnlyMemory<byte>.Empty : _buffer.AsMemory(0, Length);

    public static PooledFrame Empty => new(Array.Empty<byte>(), 0, isClosed: false);

    public static PooledFrame Closed => new(Array.Empty<byte>(), 0, isClosed: true);

    public static PooledFrame FromRentedBuffer(byte[] buffer, int length)
    {
        ArgumentNullException.ThrowIfNull(buffer);

        if (length < 0 || length > buffer.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(length));
        }

        return new PooledFrame(buffer, length, isClosed: false);
    }

    public void Dispose()
    {
        var buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer is null || ReferenceEquals(buffer, Array.Empty<byte>()))
        {
            return;
        }

        ArrayPool<byte>.Shared.Return(buffer);
    }
}
