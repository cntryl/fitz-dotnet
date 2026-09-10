
using System.Runtime.CompilerServices;

namespace Cntryl.Fitz.Runtime;

[System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "The small semaphore remains valid for concurrent callers racing terminal disposal.")]
public abstract class SubscriptionHandle : IAsyncDisposable
{
    readonly Func<CancellationToken, ValueTask> _unsubscribe;
    readonly TaskCompletionSource? _ownedCompletion;
    readonly SemaphoreSlim _unsubscribeGate = new(1, 1);
    int _unsubscribed;

    protected SubscriptionHandle(
        string pattern,
        Func<CancellationToken, ValueTask> unsubscribe,
        Task? completion = null)
    {
        Pattern = pattern;
        _unsubscribe = unsubscribe;
        if (completion is null)
        {
            _ownedCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Completion = _ownedCompletion.Task;
        }
        else
        {
            Completion = completion;
        }
    }

    public string Pattern { get; }

    /// <summary>
    /// Completes after normal unsubscribe and faults if local notification
    /// delivery terminates, including async-handler queue overflow.
    /// </summary>
    public Task Completion { get; }

    public ValueTask UnsubscribeAsync(CancellationToken cancellationToken = default) => UnsubscribeCoreAsync(cancellationToken);

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Subscription disposal is bounded best-effort cleanup and must not replace an exception leaving an await-using scope.")]
    public async ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        try
        {
            using var cleanupDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await UnsubscribeCoreAsync(cleanupDeadline.Token).ConfigureAwait(false);
        }
        catch
        {
            // Best-effort cleanup must not replace an exception leaving an await-using scope.
        }
    }

    async ValueTask UnsubscribeCoreAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _unsubscribed) != 0)
        {
            return;
        }

        await _unsubscribeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _unsubscribed) != 0)
            {
                return;
            }

            await _unsubscribe(cancellationToken).ConfigureAwait(false);
            Volatile.Write(ref _unsubscribed, 1);
            _ownedCompletion?.TrySetResult();
        }
        finally
        {
            _unsubscribeGate.Release();
        }
    }
}

public abstract class SubscriptionHandle<T> : SubscriptionHandle, IAsyncEnumerable<T>
{
    readonly IAsyncEnumerable<T> _notifications;
    int _enumerating;

    protected SubscriptionHandle(
        string pattern,
        IAsyncEnumerable<T> notifications,
        Func<CancellationToken, ValueTask> unsubscribe,
        Task? completion = null)
        : base(pattern, unsubscribe, completion)
    {
        _notifications = notifications;
    }

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _enumerating, 1, 0) != 0)
        {
            throw new InvalidOperationException("Concurrent enumeration of a subscription is not supported.");
        }

        return Enumerate(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    async IAsyncEnumerable<T> Enumerate([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var notification in _notifications.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                yield return notification;
            }
        }
        finally
        {
            Volatile.Write(ref _enumerating, 0);
        }
    }

    protected static async IAsyncEnumerable<T> EmptyNotifications()
    {
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}

public sealed class SubscriptionBackpressureException : Exception
{
    public SubscriptionBackpressureException()
    {
    }

    public SubscriptionBackpressureException(string message)
        : base(message)
    {
    }

    public SubscriptionBackpressureException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

public sealed class AsyncHandlerOverflowException : Exception
{
    public const string ErrorCode = "ASYNC_HANDLER_OVERFLOW";

    public AsyncHandlerOverflowException()
        : this("The async handler queue overflowed.")
    {
    }

    public AsyncHandlerOverflowException(string message)
        : base(message)
    {
        Domain = "unknown";
        Subscription = "unknown";
    }

    public AsyncHandlerOverflowException(string message, Exception innerException)
        : base(message, innerException)
    {
        Domain = "unknown";
        Subscription = "unknown";
    }

    public AsyncHandlerOverflowException(string domain, string subscription)
        : base($"The async handler queue overflowed for {domain} subscription '{subscription}'.")
    {
        Domain = domain;
        Subscription = subscription;
    }

    public string Code { get; } = ErrorCode;
    public string Domain { get; }
    public string Subscription { get; }
}
