using System.Threading.Channels;

namespace Cntryl.Fitz.Runtime;

internal sealed class AsyncSubscriptionBuffer<T>(string pattern, int capacity = 64)
{
    private readonly Channel<T> _channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
    {
        FullMode = BoundedChannelFullMode.Wait,
        SingleReader = true,
        SingleWriter = false,
    });

    internal void Write(T notification)
    {
        if (!_channel.Writer.TryWrite(notification))
        {
            _channel.Writer.TryComplete(new SubscriptionBackpressureException(
                $"The local subscription buffer for '{pattern}' is full"));
        }
    }

    internal void Complete() => _channel.Writer.TryComplete();

    internal IAsyncEnumerable<T> ReadAllAsync(CancellationToken cancellationToken = default) =>
        _channel.Reader.ReadAllAsync(cancellationToken);
}
