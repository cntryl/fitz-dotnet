namespace Cntryl.Fitz.Runtime;

using System.Runtime.CompilerServices;

public abstract class SubscriptionHandle : IAsyncDisposable
{
    private readonly Func<CancellationToken, ValueTask> _unsubscribe;
    private int _unsubscribed;

    protected SubscriptionHandle(
        string pattern,
        Func<CancellationToken, ValueTask> unsubscribe)
    {
        Pattern = pattern;
        _unsubscribe = unsubscribe;
    }

    public string Pattern { get; }

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

        await _unsubscribe(cancellationToken).ConfigureAwait(false);
    }
}

public abstract class SubscriptionHandle<T> : SubscriptionHandle, IAsyncEnumerable<T>
{
    private readonly IAsyncEnumerable<T> _notifications;

    protected SubscriptionHandle(
        string pattern,
        IAsyncEnumerable<T> notifications,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(pattern, unsubscribe)
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
