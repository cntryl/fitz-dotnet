namespace Cntryl.Fitz.Runtime;

using System.Runtime.CompilerServices;

public abstract class SubscriptionHandle : IAsyncDisposable
{
    private readonly Func<CancellationToken, ValueTask> _unsubscribe;
    private readonly TaskCompletionSource? _ownedCompletion;
    private int _unsubscribed;

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

    public ValueTask UnsubscribeAsync(CancellationToken cancellationToken = default)
    {
        return UnsubscribeCoreAsync(cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return UnsubscribeCoreAsync(CancellationToken.None);
    }

    private async ValueTask UnsubscribeCoreAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _unsubscribed, 1) != 0)
        {
            return;
        }

        try
        {
            await _unsubscribe(cancellationToken).ConfigureAwait(false);
            _ownedCompletion?.TrySetResult();
        }
        catch (Exception exception)
        {
            _ownedCompletion?.TrySetException(exception);
            throw;
        }
    }
}

public abstract class SubscriptionHandle<T> : SubscriptionHandle, IAsyncEnumerable<T>
{
    private readonly IAsyncEnumerable<T> _notifications;

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
        return _notifications.GetAsyncEnumerator(cancellationToken);
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
