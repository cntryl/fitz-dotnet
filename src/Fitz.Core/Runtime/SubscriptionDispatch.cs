using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;

namespace Cntryl.Fitz.Runtime;

delegate bool AsyncHandlerDispatch(
    Func<CancellationToken, ValueTask> handler,
    Action<Exception>? onRejected);

sealed class SubscriptionRegistration<TNotification> : IDisposable
{
    internal const int DefaultCapacity = 256;
    int _disposed;
    CancellationTokenSource? _cancellationSource;
    readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly string _domain;
    readonly string _pattern;
    readonly Func<CancellationToken, ValueTask>? _overflowCleanup;

    internal SubscriptionRegistration(
        Channel<TNotification> channel,
        string domain = "subscription",
        string pattern = "unknown",
        Func<CancellationToken, ValueTask>? overflowCleanup = null,
        Action<TNotification>? preDispatch = null)
    {
        Channel = channel;
        _domain = domain;
        _pattern = pattern;
        _overflowCleanup = overflowCleanup;
        PreDispatch = preDispatch;
        _ = _completion.Task.ContinueWith(
            static task => _ = task.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    internal Channel<TNotification> Channel { get; }

    internal Action<TNotification>? PreDispatch { get; }

    internal bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    internal Task Completion => _completion.Task;

    internal CancellationToken CancellationToken => GetOrCreateCancellationSource().Token;

    internal static Channel<TNotification> CreateChannel(int capacity = DefaultCapacity) =>
        System.Threading.Channels.Channel.CreateBounded<TNotification>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
        });

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        var cancellationSource = Volatile.Read(ref _cancellationSource);
        cancellationSource?.Cancel();
        Channel.Writer.TryComplete();
        _completion.TrySetResult();
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Terminal subscription cleanup is best effort and its overflow signal must remain authoritative.")]
    internal Task FailOverflowAsync() => FailAsync(new AsyncHandlerOverflowException(_domain, _pattern));

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Terminal subscription cleanup is best effort and the original failure must remain authoritative.")]
    internal async Task FailAsync(Exception error)
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        var cancellationSource = Volatile.Read(ref _cancellationSource);
        if (cancellationSource is not null)
        {
            await cancellationSource.CancelAsync().ConfigureAwait(false);
        }
        Channel.Writer.TryComplete(error);
        _completion.TrySetException(error);

        if (_overflowCleanup is not null)
        {
            try
            {
                using var cleanupDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await _overflowCleanup(cleanupDeadline.Token).ConfigureAwait(false);
            }
            catch
            {
            }
        }
    }

    CancellationTokenSource GetOrCreateCancellationSource()
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

    internal void DisposeCancellationSource() =>
        Interlocked.Exchange(ref _cancellationSource, null)?.Dispose();
}

static class SubscriptionPump
{
    internal static void Start<TNotification>(
        SubscriptionRegistration<TNotification> registration,
        Func<TNotification, CancellationToken, ValueTask> handler,
        AsyncHandlerDispatch? dispatch = null)
    {
        ThreadPool.UnsafeQueueUserWorkItem(
            static state => _ = RunAsync(state.Registration, state.Handler, state.Dispatch),
            new PumpState<TNotification>(registration, handler, dispatch),
            preferLocal: false);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Subscription pumps isolate arbitrary user callbacks by faulting only their registration.")]
    static async Task RunAsync<TNotification>(
        SubscriptionRegistration<TNotification> registration,
        Func<TNotification, CancellationToken, ValueTask> handler,
        AsyncHandlerDispatch? dispatch)
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
                            var callbackCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                            if (!dispatch(async dispatcherToken =>
                            {
                                try
                                {
                                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                                        registration.CancellationToken,
                                        dispatcherToken);
                                    await handler(message, linkedCts.Token).ConfigureAwait(false);
                                    callbackCompletion.TrySetResult();
                                }
                                catch (Exception exception)
                                {
                                    callbackCompletion.TrySetException(exception);
                                    throw;
                                }
                            }, exception => callbackCompletion.TrySetException(exception)))
                            {
                                await registration.FailOverflowAsync().ConfigureAwait(false);
                                return;
                            }

                            await callbackCompletion.Task.ConfigureAwait(false);
                        }
                    }
                    catch (OperationCanceledException) when (registration.IsDisposed)
                    {
                        return;
                    }
                    catch (Exception exception)
                    {
                        await registration.FailAsync(exception).ConfigureAwait(false);
                        return;
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
        finally
        {
            registration.DisposeCancellationSource();
        }
    }

    readonly record struct PumpState<TNotification>(
        SubscriptionRegistration<TNotification> Registration,
        Func<TNotification, CancellationToken, ValueTask> Handler,
        AsyncHandlerDispatch? Dispatch);
}
