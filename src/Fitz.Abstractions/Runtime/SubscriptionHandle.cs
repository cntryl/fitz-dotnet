using System.Runtime.CompilerServices;

namespace Cntryl.Fitz.Runtime;

/// <summary>
/// Base class for subscription handles: owns the pattern, the completion signal, and
/// exactly-once unsubscribe.
/// </summary>
/// <remarks>
/// Disposal unsubscribes on a bounded best-effort basis and never replaces an exception
/// already leaving an <c>await using</c> scope. Unsubscribing twice is safe.
/// </remarks>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "The small semaphore remains valid for concurrent callers racing terminal disposal.")]
public abstract class SubscriptionHandle : IAsyncDisposable
{
    readonly Func<CancellationToken, ValueTask> _unsubscribe;
    readonly TaskCompletionSource? _ownedCompletion;
    readonly SemaphoreSlim _unsubscribeGate = new(1, 1);
    int _unsubscribed;

    /// <summary>
    /// Initializes the handle.
    /// </summary>
    /// <param name="pattern">Route or pattern this subscription was registered with.</param>
    /// <param name="unsubscribe">Callback that withdraws the registration from the broker.</param>
    /// <param name="completion">
    /// Externally owned completion signal, or <see langword="null"/> to have the handle own one.
    /// </param>
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
    /// <remarks>An unsubscribe the broker rejects remains retryable.</remarks>
    public ValueTask UnsubscribeAsync(CancellationToken ct = default) => UnsubscribeCoreAsync(ct);

    /// <summary>
    /// Unsubscribes on a best-effort basis, bounded to five seconds, and swallows failures.
    /// </summary>
    /// <returns>A task that completes once cleanup finishes.</returns>
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

    async ValueTask UnsubscribeCoreAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _unsubscribed) != 0)
        {
            return;
        }

        await _unsubscribeGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _unsubscribed) != 0)
            {
                return;
            }

            await _unsubscribe(ct).ConfigureAwait(false);
            Volatile.Write(ref _unsubscribed, 1);
            _ownedCompletion?.TrySetResult();
        }
        finally
        {
            _unsubscribeGate.Release();
        }
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
    protected SubscriptionHandle(
        string pattern,
        IAsyncEnumerable<T> notifications,
        Func<CancellationToken, ValueTask> unsubscribe,
        Task? completion = null)
        : base(pattern, unsubscribe, completion)
    {
        _notifications = notifications;
    }

    /// <summary>
    /// Begins enumerating notifications.
    /// </summary>
    /// <param name="ct">Cancellation token that stops enumeration.</param>
    /// <returns>An enumerator over this subscription's notifications.</returns>
    /// <exception cref="InvalidOperationException">The handle is already being enumerated.</exception>
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken ct = default)
    {
        if (Interlocked.CompareExchange(ref _enumerating, 1, 0) != 0)
        {
            throw new InvalidOperationException("Concurrent enumeration of a subscription is not supported.");
        }

        return Enumerate(ct).GetAsyncEnumerator(ct);
    }

    async IAsyncEnumerable<T> Enumerate([EnumeratorCancellation] CancellationToken ct)
    {
        try
        {
            await foreach (var notification in _notifications.WithCancellation(ct).ConfigureAwait(false))
            {
                yield return notification;
            }
        }
        finally
        {
            Volatile.Write(ref _enumerating, 0);
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
public sealed class SubscriptionBackpressureException : Exception
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
public sealed class AsyncHandlerOverflowException : Exception
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
