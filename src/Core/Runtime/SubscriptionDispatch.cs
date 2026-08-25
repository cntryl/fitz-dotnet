using System.Threading.Channels;
using System.Diagnostics.CodeAnalysis;

namespace Cntryl.Fitz.Runtime;

internal sealed class SubscriptionRegistration<TNotification> : IDisposable
{
    private int _disposed;
    private CancellationTokenSource? _cancellationSource;
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly string _domain;
    private readonly string _pattern;
    private readonly Func<CancellationToken, ValueTask>? _overflowCleanup;

    internal SubscriptionRegistration(
        Channel<TNotification> channel,
        string domain = "subscription",
        string pattern = "unknown",
        Func<CancellationToken, ValueTask>? overflowCleanup = null)
    {
        Channel = channel;
        _domain = domain;
        _pattern = pattern;
        _overflowCleanup = overflowCleanup;
        _ = _completion.Task.ContinueWith(
            static task => _ = task.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    internal Channel<TNotification> Channel { get; }

    internal bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    internal Task Completion => _completion.Task;

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
        _completion.TrySetResult();
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Terminal subscription cleanup is best effort and its overflow signal must remain authoritative.")]
    internal async ValueTask FailOverflowAsync()
    {
        var error = new AsyncHandlerOverflowException(_domain, _pattern);
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        var cancellationSource = Interlocked.Exchange(ref _cancellationSource, null);
        if (cancellationSource is not null)
        {
            await cancellationSource.CancelAsync().ConfigureAwait(false);
        }
        Channel.Writer.TryComplete(error);
        cancellationSource?.Dispose();
        _completion.TrySetException(error);

        if (_overflowCleanup is not null)
        {
            try
            {
                await _overflowCleanup(CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
            }
        }
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

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Subscription pumps isolate arbitrary user callback and channel shutdown failures.")]
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
                            if (!dispatch(async dispatcherToken =>
                            {
                                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                                    registration.CancellationToken,
                                    dispatcherToken);
                                await handler(message, linkedCts.Token).ConfigureAwait(false);
                            }))
                            {
                                await registration.FailOverflowAsync().ConfigureAwait(false);
                                return;
                            }
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
