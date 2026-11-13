namespace Cntryl.Fitz;

/// <summary>
/// Base class for subscription handles: owns the pattern, the completion signal, and
/// exactly-once unsubscribe.
/// </summary>
/// <remarks>
/// Disposal unsubscribes on a bounded best-effort basis and never replaces an exception
/// already leaving an <c>await using</c> scope. Unsubscribing twice is safe.
/// </remarks>
public abstract class SubscriptionHandle : IAsyncDisposable
{
    static readonly TimeSpan CleanupBudget = TimeSpan.FromSeconds(5);

    readonly Func<CancellationToken, ValueTask> _unsubscribe;
    readonly TaskCompletionSource? _ownedCompletion;
    readonly SemaphoreSlim _unsubscribeGate = new(1, 1);
    readonly CancellationTokenSource _closing = new();
    readonly CancellationToken _closingToken;
    readonly TimeProvider _time;
    int _unsubscribed;
    int _disposed;

    /// <summary>
    /// Initializes the handle.
    /// </summary>
    /// <param name="pattern">Route or pattern this subscription was registered with.</param>
    /// <param name="unsubscribe">Callback that withdraws the registration from the broker.</param>
    /// <param name="completion">
    /// Externally owned completion signal, or <see langword="null"/> to have the handle own one.
    /// </param>
    /// <param name="timeProvider">The clock that bounds cleanup on disposal; the system clock by default.</param>
    protected SubscriptionHandle(
        string pattern,
        Func<CancellationToken, ValueTask> unsubscribe,
        Task? completion = null,
        TimeProvider? timeProvider = null)
    {
        Pattern = pattern;
        _time = timeProvider ?? TimeProvider.System;
        _unsubscribe = unsubscribe;
        _closingToken = _closing.Token;
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

    /// <summary>The route or pattern this subscription was registered with.</summary>
    public string Pattern { get; }

    /// <summary>
    /// Completes after normal unsubscribe and faults if local notification
    /// delivery terminates, including async-handler queue overflow.
    /// </summary>
    public Task Completion { get; }

    /// <summary>
    /// Withdraws the registration from the broker. Safe to call more than once.
    /// </summary>
    /// <param name="ct">Cancellation token bounding the unsubscribe request.</param>
    /// <returns>A task that completes once the broker accepts the unsubscribe.</returns>
    /// <remarks>
    /// An unsubscribe the broker rejects remains retryable. Once the handle is disposed this
    /// returns without contacting the broker, because disposal already unsubscribed.
    /// </remarks>
    public async ValueTask UnsubscribeAsync(CancellationToken ct = default)
    {
        if (Volatile.Read(ref _unsubscribed) != 0)
            return;

        if (!await _unsubscribeGate.TryEnterAsync(_closingToken, ct).ConfigureAwait(false))
            return;

        try
        {
            await UnsubscribeOnceAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _unsubscribeGate.Release();
        }
    }

    /// <summary>
    /// Unsubscribes on a best-effort basis, bounded to five seconds, and swallows failures.
    /// </summary>
    /// <returns>A task that completes once cleanup finishes.</returns>
    /// <remarks>
    /// Waits for an in-progress <see cref="UnsubscribeAsync"/> to finish first. Calls that are
    /// still queued, or that start afterwards, return without contacting the broker.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        GC.SuppressFinalize(this);
        await _closing.CancelAsync().ConfigureAwait(false);
        await _unsubscribeGate.DrainAndDisposeAsync().ConfigureAwait(false);
        _closing.Dispose();
        try
        {
            using var cleanupDeadline = new CancellationTokenSource(CleanupBudget, _time);
            await UnsubscribeOnceAsync(cleanupDeadline.Token).ConfigureAwait(false);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            // Best-effort cleanup must not replace an exception leaving an await-using scope.
        }
    }

    // Callers hold the gate, or have drained it during disposal.
    async ValueTask UnsubscribeOnceAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _unsubscribed) != 0)
            return;

        await _unsubscribe(ct).ConfigureAwait(false);
        Volatile.Write(ref _unsubscribed, 1);
        _ownedCompletion?.TrySetResult();
    }
}

/// <summary>
/// A subscription handle that yields notifications by <c>await foreach</c>.
/// </summary>
/// <typeparam name="T">Notification type delivered by this subscription.</typeparam>
/// <remarks>
/// Each handle has its own bounded buffer. A consumer too slow to keep up terminates with
/// <see cref="SubscriptionBackpressureException"/> without affecting sibling handles.
/// Only one enumeration at a time is permitted.
/// </remarks>
public abstract class SubscriptionHandle<T> : SubscriptionHandle, IAsyncEnumerable<T>
{
    readonly IAsyncEnumerable<T> _notifications;
    int _enumerating;

    /// <summary>
    /// Initializes the handle.
    /// </summary>
    /// <param name="pattern">Route or pattern this subscription was registered with.</param>
    /// <param name="notifications">Bounded source of notifications for this subscription.</param>
    /// <param name="unsubscribe">Callback that withdraws the registration from the broker.</param>
    /// <param name="completion">
    /// Externally owned completion signal, or <see langword="null"/> to have the handle own one.
    /// </param>
    /// <param name="timeProvider">The clock that bounds cleanup on disposal; the system clock by default.</param>
    protected SubscriptionHandle(
        string pattern,
        IAsyncEnumerable<T> notifications,
        Func<CancellationToken, ValueTask> unsubscribe,
        Task? completion = null,
        TimeProvider? timeProvider = null)
        : base(pattern, unsubscribe, completion, timeProvider)
    {
        _notifications = notifications;
    }

    /// <summary>
    /// Begins enumerating notifications.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token that stops enumeration.</param>
    /// <returns>An enumerator over this subscription's notifications.</returns>
    /// <exception cref="InvalidOperationException">The handle is already being enumerated.</exception>
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _enumerating, 1, 0) != 0)
        {
            throw new InvalidOperationException("Concurrent enumeration of a subscription is not supported.");
        }

        var transferred = false;
        try
        {
            var enumerator = new SubscriptionEnumerator(this, _notifications.GetAsyncEnumerator(cancellationToken));
            transferred = true;
            return enumerator;
        }
        finally
        {
            if (!transferred)
                Volatile.Write(ref _enumerating, 0);
        }
    }

    sealed class SubscriptionEnumerator(SubscriptionHandle<T> owner, IAsyncEnumerator<T> inner) : IAsyncEnumerator<T>
    {
        int _disposed;
        int _released;

        public T Current => inner.Current;

        public async ValueTask<bool> MoveNextAsync()
        {
            if (Volatile.Read(ref _disposed) != 0)
                return false;

            var hasNext = false;
            try
            {
                hasNext = await inner.MoveNextAsync().ConfigureAwait(false);
                return hasNext;
            }
            finally
            {
                if (!hasNext)
                    ReleaseGuard();
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            try
            {
                await inner.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                ReleaseGuard();
            }
        }

        void ReleaseGuard()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                Volatile.Write(ref owner._enumerating, 0);
        }
    }

    /// <summary>
    /// A notification source that completes immediately, for handles with nothing to deliver.
    /// </summary>
    /// <returns>An empty asynchronous sequence.</returns>
    protected static async IAsyncEnumerable<T> EmptyNotifications()
    {
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}

/// <summary>
/// Thrown into an enumeration when a subscription's bounded buffer overflows because the
/// consumer could not keep up. Terminates only that handle.
/// </summary>
/// <remarks>
/// Raise <c>AsyncHandlerOptions.SubscriptionBufferCapacity</c> or consume faster. The
/// default capacity is 256 notifications.
/// </remarks>
public sealed class SubscriptionBackpressureException : FitzException
{
    /// <summary>Initializes the exception with a default message.</summary>
    public SubscriptionBackpressureException()
    {
    }

    /// <summary>Initializes the exception with a message.</summary>
    /// <param name="message">Description of the overflow.</param>
    public SubscriptionBackpressureException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception with a message and underlying cause.</summary>
    /// <param name="message">Description of the overflow.</param>
    /// <param name="innerException">The underlying cause.</param>
    public SubscriptionBackpressureException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Faults a subscription's <see cref="SubscriptionHandle.Completion"/> when the shared
/// callback delivery queue overflows, and terminates the local registration.
/// </summary>
/// <remarks>
/// Distinct from <see cref="SubscriptionBackpressureException"/>, which is per-handle.
/// This one indicates the client-wide callback queue, sized by
/// <c>AsyncHandlerOptions.QueueCapacity</c>, could not absorb delivery.
/// </remarks>
public sealed class AsyncHandlerOverflowException : FitzException
{
    /// <summary>Stable error code identifying async-handler overflow.</summary>
    public const string ErrorCode = "ASYNC_HANDLER_OVERFLOW";

    /// <summary>Initializes the exception with a default message.</summary>
    public AsyncHandlerOverflowException()
        : this("The async handler queue overflowed.")
    {
    }

    /// <summary>Initializes the exception with a message.</summary>
    /// <param name="message">Description of the overflow.</param>
    public AsyncHandlerOverflowException(string message)
        : base(message)
    {
        Domain = "unknown";
        Subscription = "unknown";
    }

    /// <summary>Initializes the exception with a message and underlying cause.</summary>
    /// <param name="message">Description of the overflow.</param>
    /// <param name="innerException">The underlying cause.</param>
    public AsyncHandlerOverflowException(string message, Exception innerException)
        : base(message, innerException)
    {
        Domain = "unknown";
        Subscription = "unknown";
    }

    /// <summary>Initializes the exception for a specific domain and subscription.</summary>
    /// <param name="domain">Domain whose handler queue overflowed.</param>
    /// <param name="subscription">Route or pattern of the affected subscription.</param>
    public AsyncHandlerOverflowException(string domain, string subscription)
        : base($"The async handler queue overflowed for {domain} subscription '{subscription}'.")
    {
        Domain = domain;
        Subscription = subscription;
    }

    /// <summary>Always <see cref="ErrorCode"/>.</summary>
    public string Code { get; } = ErrorCode;

    /// <summary>Domain whose handler queue overflowed, or <c>"unknown"</c>.</summary>
    public string Domain { get; }

    /// <summary>Route or pattern of the affected subscription, or <c>"unknown"</c>.</summary>
    public string Subscription { get; }
}
