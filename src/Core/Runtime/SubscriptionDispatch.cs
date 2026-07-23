using System.Threading.Channels;

namespace Cntryl.Fitz.Runtime;

internal sealed class SubscriptionRegistration<TNotification> : IDisposable
{
    private int _disposed;
    private CancellationTokenSource? _cancellationSource;

    internal SubscriptionRegistration(Channel<TNotification> channel)
    {
        Channel = channel;
    }

    internal Channel<TNotification> Channel { get; }

    internal bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    internal CancellationToken CancellationToken => GetOrCreateCancellationSource().Token;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        var cancellationSource = Interlocked.Exchange(ref _cancellationSource, null);
        cancellationSource?.Cancel();
        Channel.Writer.TryComplete();
        cancellationSource?.Dispose();
    }

    private CancellationTokenSource GetOrCreateCancellationSource()
    {
        var existing = Volatile.Read(ref _cancellationSource);
        if (existing is not null)
        {
            return existing;
        }

        var created = new CancellationTokenSource();
        var prior = Interlocked.CompareExchange(ref _cancellationSource, created, comparand: null);
        if (prior is not null)
        {
            created.Dispose();
            return prior;
        }

        if (IsDisposed)
        {
            created.Cancel();
        }

        return created;
    }
}

internal static class SubscriptionPump
{
    internal static void Start<TNotification>(
        SubscriptionRegistration<TNotification> registration,
        Func<TNotification, CancellationToken, ValueTask> handler,
        Func<Func<CancellationToken, ValueTask>, bool>? dispatch = null)
    {
        ThreadPool.UnsafeQueueUserWorkItem(
            static state => _ = RunAsync(state.Registration, state.Handler, state.Dispatch),
            new PumpState<TNotification>(registration, handler, dispatch),
            preferLocal: false);
    }

    private static async Task RunAsync<TNotification>(
        SubscriptionRegistration<TNotification> registration,
        Func<TNotification, CancellationToken, ValueTask> handler,
        Func<Func<CancellationToken, ValueTask>, bool>? dispatch)
    {
        try
        {
            while (await registration.Channel.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                while (registration.Channel.Reader.TryRead(out var message))
                {
                    if (registration.IsDisposed)
                    {
                        return;
                    }

                    try
                    {
                        if (dispatch is null)
                        {
                            await handler(message, registration.CancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            _ = dispatch(async dispatcherToken =>
                            {
                                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                                    registration.CancellationToken,
                                    dispatcherToken);
                                await handler(message, linkedCts.Token).ConfigureAwait(false);
                            });
                        }
                    }
                    catch (OperationCanceledException) when (registration.IsDisposed)
                    {
                        return;
                    }
                    catch
                    {
                    }

                    if (registration.IsDisposed)
                    {
                        return;
                    }
                }
            }
        }
        catch
        {
        }
    }

    private readonly record struct PumpState<TNotification>(
        SubscriptionRegistration<TNotification> Registration,
        Func<TNotification, CancellationToken, ValueTask> Handler,
        Func<Func<CancellationToken, ValueTask>, bool>? Dispatch);
}
