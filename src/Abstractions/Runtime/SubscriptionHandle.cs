namespace Cntryl.Fitz.Runtime;

public abstract class SubscriptionHandle : IAsyncDisposable
{
    private readonly Func<CancellationToken, ValueTask> _unsubscribe;
    private int _unsubscribed;

    protected SubscriptionHandle(
        ulong subscriptionId,
        string pattern,
        Func<CancellationToken, ValueTask> unsubscribe)
    {
        SubscriptionId = subscriptionId;
        Pattern = pattern;
        _unsubscribe = unsubscribe;
    }

    public ulong SubscriptionId { get; }

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