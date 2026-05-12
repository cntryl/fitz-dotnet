namespace Cntryl.Fitz.Runtime;

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
