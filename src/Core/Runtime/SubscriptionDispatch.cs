using System.Threading.Channels;

namespace Cntryl.Fitz.Runtime;

internal sealed class SubscriptionRegistration<TNotification> : IDisposable
{
    private int _disposed;

    internal SubscriptionRegistration(Channel<TNotification> channel)
    {
        Channel = channel;
        CancellationSource = new CancellationTokenSource();
    }

    internal Channel<TNotification> Channel { get; }

    internal CancellationToken CancellationToken => CancellationSource.Token;

    private CancellationTokenSource CancellationSource { get; }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        CancellationSource.Cancel();
        Channel.Writer.TryComplete();
        CancellationSource.Dispose();
    }
}

internal static class SubscriptionPump
{
    internal static void Start<TNotification>(
        SubscriptionRegistration<TNotification> registration,
        Func<TNotification, CancellationToken, ValueTask> handler)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                while (await registration.Channel.Reader.WaitToReadAsync(registration.CancellationToken).ConfigureAwait(false))
                {
                    while (registration.Channel.Reader.TryRead(out var message))
                    {
                        try
                        {
                            await handler(message, registration.CancellationToken).ConfigureAwait(false);
                        }
                        catch
                        {
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch
            {
            }
        });
    }
}