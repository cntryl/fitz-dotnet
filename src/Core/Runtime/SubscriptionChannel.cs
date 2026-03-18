using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Cntryl.Fitz.Runtime;

/// <summary>
/// A reusable async enumerable channel for server-sent subscription notifications.
/// Bridges between Multiplexer notification handlers and consumer async enumeration.
/// </summary>
internal sealed class SubscriptionChannel<T>
{
    private readonly Channel<T> _channel;
    private bool _disposed;

    internal SubscriptionChannel()
    {
        _channel = Channel.CreateUnbounded<T>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = true,
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

        // Channel is closed or disposed
        Dispose();
    }

    /// <summary>
    /// Gets the async enumerable for consuming notifications.
    /// </summary>
    internal async IAsyncEnumerable<T> GetEnumerableAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (_channel.Reader.TryRead(out var notification))
            {
                yield return notification;
            }
        }
    }

    internal async ValueTask<SubscriptionReadResult<T>> ReadAsync(CancellationToken cancellationToken = default)
    {
        while (await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
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
    internal void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _channel.Writer.TryComplete();
    }
}

internal readonly record struct SubscriptionReadResult<T>(bool HasItem, T Item);
