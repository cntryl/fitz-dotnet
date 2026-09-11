using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Cntryl.Fitz.Runtime;

/// <summary>
/// A reusable async enumerable channel for server-sent subscription notifications.
/// Bridges between Multiplexer notification handlers and consumer async enumeration.
/// </summary>
sealed class SubscriptionChannel<T>
{
    readonly Channel<T> _channel;
    bool _disposed;

    internal SubscriptionChannel(int capacity = SubscriptionRegistration<T>.DefaultCapacity)
    {
        _channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
        {
            SingleReader = false,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    /// <summary>
    /// Posts a notification to the channel. Called by Multiplexer notification handler.
    /// </summary>
    internal void PostNotification(T notification)
    {
        if (!_disposed && _channel.Writer.TryWrite(notification))
        {
            return;
        }

        _disposed = true;
        _channel.Writer.TryComplete(new SubscriptionBackpressureException(
            "The local RPC response buffer is full"));
    }

    /// <summary>
    /// Gets the async enumerable for consuming notifications.
    /// </summary>
    internal async IAsyncEnumerable<T> GetEnumerableAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        while (await _channel.Reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            while (_channel.Reader.TryRead(out var notification))
            {
                yield return notification;
            }
        }
    }

    internal async ValueTask<SubscriptionReadResult<T>> ReadAsync(CancellationToken ct = default)
    {
        while (await _channel.Reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            if (_channel.Reader.TryRead(out var notification))
            {
                return new SubscriptionReadResult<T>(true, notification);
            }
        }

        return new SubscriptionReadResult<T>(false, default!);
    }

    /// <summary>
    /// Closes the channel and completes all pending enumerations.
    /// Called by subscription cleanup (on disconnect or explicit unsubscribe).
    /// </summary>
    internal void Dispose() => Complete();

    internal void Complete(Exception? error = null)
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _channel.Writer.TryComplete(error);
    }
}

readonly record struct SubscriptionReadResult<T>(bool HasItem, T Item);
