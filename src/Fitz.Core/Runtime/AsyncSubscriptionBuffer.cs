using System.Threading.Channels;

namespace Cntryl.Fitz.Runtime;

sealed class AsyncSubscriptionBuffer<T>(string pattern, int capacity = SubscriptionRegistration<T>.DefaultCapacity)
{
    readonly Channel<T> _channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
    {
        FullMode = BoundedChannelFullMode.Wait,
        SingleReader = true,
        SingleWriter = false,
    });

    internal void Write(T notification)
    {
        if (!_channel.Writer.TryWrite(notification))
        {
            var exception = new SubscriptionBackpressureException(
                $"The local subscription buffer for '{pattern}' is full");
            _channel.Writer.TryComplete(exception);
            throw exception;
        }
    }

    internal void Complete() => _channel.Writer.TryComplete();

    internal void ObserveCompletion(Task completion) => _ = ObserveCompletionAsync(completion);

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Every terminal registration failure must be forwarded to the enumerable channel.")]
    async Task ObserveCompletionAsync(Task completion)
    {
        try
        {
            await completion.ConfigureAwait(false);
            _channel.Writer.TryComplete();
        }
        catch (Exception ex)
        {
            _channel.Writer.TryComplete(ex);
        }
    }

    internal IAsyncEnumerable<T> ReadAllAsync(CancellationToken cancellationToken = default) =>
        _channel.Reader.ReadAllAsync(cancellationToken);
}
