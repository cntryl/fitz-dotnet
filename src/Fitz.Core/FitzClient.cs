using System.IO;
using System.Net.Sockets;
using System.Net.WebSockets;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Domains.Notice;
using Cntryl.Fitz.Domains.Queue;
using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Domains.Schedule;
using Cntryl.Fitz.Domains.Stream;

namespace Cntryl.Fitz;

/// <summary>
/// The default <see cref="IFitzClient"/>: owns one broker connection and every domain API.
/// </summary>
/// <remarks>
/// Safe for concurrent use. Domain clients are created on first access and share the
/// connection. Prefer <c>await using</c>; synchronous <see cref="Dispose"/> starts the
/// asynchronous close without blocking on it.
/// </remarks>
public sealed class FitzClient : IFitzClient, IDisposable
{
    readonly ClientConfig _config;
    readonly FitzConnection _connection;
    readonly Lazy<KvClient> _kvClient;
    readonly Lazy<LeaseClient> _leaseClient;
    readonly Lazy<NoticeClient> _noticeClient;
    readonly Lazy<QueueClient> _queueClient;
    readonly Lazy<RpcClient> _rpcClient;
    readonly Lazy<ScheduleClient> _scheduleClient;
    readonly Lazy<StreamClient> _streamClient;
    bool _disposed;

    /// <summary>
    /// Creates a client. No connection is opened until you call a connect method.
    /// </summary>
    /// <param name="config">Configuration for the connection. Validated immediately.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is <see langword="null"/>.</exception>
    public FitzClient(ClientConfig config)
        : this(config, TimeProvider.System)
    {
    }

    /// <summary>Creates a client that keeps time with <paramref name="time"/>.</summary>
    /// <param name="config">Configuration for the connection. Validated immediately.</param>
    /// <param name="time">The clock for every timeout, backoff, and deadline of this client.</param>
    internal FitzClient(ClientConfig config, TimeProvider time)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        var transportFactory = _config.TransportFactory ?? TransportResolver.Resolve;
        _connection = new FitzConnection(_config, () => transportFactory(_config), time);
        _kvClient = new Lazy<KvClient>(() => new KvClient(_connection), LazyThreadSafetyMode.ExecutionAndPublication);
        _leaseClient = new Lazy<LeaseClient>(() => new LeaseClient(_connection), LazyThreadSafetyMode.ExecutionAndPublication);
        _noticeClient = new Lazy<NoticeClient>(() => new NoticeClient(_connection), LazyThreadSafetyMode.ExecutionAndPublication);
        _queueClient = new Lazy<QueueClient>(() => new QueueClient(_connection), LazyThreadSafetyMode.ExecutionAndPublication);
        _rpcClient = new Lazy<RpcClient>(() => new RpcClient(_connection), LazyThreadSafetyMode.ExecutionAndPublication);
        _scheduleClient = new Lazy<ScheduleClient>(() => new ScheduleClient(_connection), LazyThreadSafetyMode.ExecutionAndPublication);
        _streamClient = new Lazy<StreamClient>(() => new StreamClient(_connection), LazyThreadSafetyMode.ExecutionAndPublication);
    }

    /// <summary>The configuration this client was created with.</summary>
    public ClientConfig Config => _config;

    /// <inheritdoc />
    public Task ConnectAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        return _connection.ConnectAsync(ct);
    }

    /// <inheritdoc />
    public async Task ConnectWhenReadyAsync(ConnectWhenReadyOptions? options = null, CancellationToken ct = default)
    {
        const string TimedOut = "Timed out waiting for Fitz to become ready.";
        ThrowIfDisposed();

        options ??= new ConnectWhenReadyOptions();
        var timeout = ResolveDelay(options.Timeout, _config.Timeout ?? TimeSpan.FromSeconds(30), allowZero: true);
        var backoff = ResolveDelay(options.Backoff, TimeSpan.FromMilliseconds(250));
        var maxBackoff = ResolveDelay(options.MaxBackoff, TimeSpan.FromSeconds(2));
        var time = _connection.Time;

        // A zero timeout means one attempt and no retries. Otherwise one deadline covers the whole
        // call, and "did it time out?" is read from that deadline's own cancellation. Comparing a
        // clock reading with the deadline instead can disagree with the timer by a fraction of a
        // millisecond and report a timeout as a plain cancellation.
        var singleAttempt = timeout == TimeSpan.Zero;
        using var deadline = singleAttempt || timeout == System.Threading.Timeout.InfiniteTimeSpan
            ? null
            : new CancellationTokenSource(timeout, time);
        using var bounded = deadline is null ? null : CancellationTokenSource.CreateLinkedTokenSource(ct, deadline.Token);
        var attemptToken = bounded?.Token ?? ct;
        var attempts = 0;

        while (true)
        {
            ThrowIfDisposed();
            ct.ThrowIfCancellationRequested();
            if (++attempts > 1 && Expired())
            {
                throw new TimeoutException(TimedOut);
            }

            try
            {
                await ConnectAsync(attemptToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException cancellation) when (!ct.IsCancellationRequested && Expired())
            {
                throw new TimeoutException(TimedOut, cancellation);
            }
            catch (AuthenticationException)
            {
                throw;
            }
            catch (ConnectionException) when (Volatile.Read(ref _disposed) || State == ConnectionState.Closed)
            {
                throw;
            }
            catch (Exception attemptFailure) when (Expired())
            {
                throw new TimeoutException(TimedOut, attemptFailure);
            }
            catch (Exception exception) when (IsStartupReadinessFailure(State) && IsTransientStartupFailure(exception))
            {
                try
                {
                    await Task.Delay(backoff, time, attemptToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException cancellation) when (!ct.IsCancellationRequested && Expired())
                {
                    throw new TimeoutException(TimedOut, cancellation);
                }

                backoff = Min(TimeSpan.FromMilliseconds(backoff.TotalMilliseconds * 2), maxBackoff);
            }
        }

        bool Expired() => singleAttempt || deadline is { IsCancellationRequested: true };
    }

    /// <inheritdoc />
    public async Task CloseAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (Interlocked.Exchange(ref _disposed, true))
        {
            return;
        }
        try
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            if (_kvClient.IsValueCreated)
                await _kvClient.Value.DisposeAsync().ConfigureAwait(false);
            if (_leaseClient.IsValueCreated)
                await _leaseClient.Value.DisposeAsync().ConfigureAwait(false);
            if (_noticeClient.IsValueCreated)
                await _noticeClient.Value.DisposeAsync().ConfigureAwait(false);
            if (_queueClient.IsValueCreated)
                await _queueClient.Value.DisposeAsync().ConfigureAwait(false);
            if (_rpcClient.IsValueCreated)
                _rpcClient.Value.Dispose();
            if (_scheduleClient.IsValueCreated)
                await _scheduleClient.Value.DisposeAsync().ConfigureAwait(false);
            if (_streamClient.IsValueCreated)
                await _streamClient.Value.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Closes the connection and releases everything it owns.</summary>
    /// <returns>A task that completes once the client is closed.</returns>
    public async ValueTask DisposeAsync() => await CloseAsync().ConfigureAwait(false);

    /// <summary>
    /// Starts closing the connection without blocking, for containers that dispose
    /// synchronously. Prefer <see cref="DisposeAsync"/> when you can await it.
    /// </summary>
    public void Dispose()
    {
        var closeTask = CloseAsync();
        if (!closeTask.IsCompletedSuccessfully)
        {
            _ = ObserveSynchronousDisposeAsync(closeTask);
        }

        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    public IKvClient Kv
    {
        get
        {
            ThrowIfDisposed();
            return _kvClient.Value;
        }
    }

    /// <inheritdoc />
    public ILeaseClient Lease
    {
        get
        {
            ThrowIfDisposed();
            return _leaseClient.Value;
        }
    }

    /// <inheritdoc />
    public INoticeClient Notice
    {
        get
        {
            ThrowIfDisposed();
            return _noticeClient.Value;
        }
    }

    /// <inheritdoc />
    public IQueueClient Queue
    {
        get
        {
            ThrowIfDisposed();
            return _queueClient.Value;
        }
    }

    /// <inheritdoc />
    public IRpcClient Rpc
    {
        get
        {
            ThrowIfDisposed();
            return _rpcClient.Value;
        }
    }

    /// <inheritdoc />
    public IScheduleClient Schedule
    {
        get
        {
            ThrowIfDisposed();
            return _scheduleClient.Value;
        }
    }

    /// <inheritdoc />
    public IStreamClient Stream
    {
        get
        {
            ThrowIfDisposed();
            return _streamClient.Value;
        }
    }

    /// <inheritdoc />
    public bool IsConnected => _connection.State == ConnectionState.Authenticated;

    /// <summary>Numeric capabilities negotiated for the current live session.</summary>
    public IReadOnlyList<uint> CapabilityIds => IsConnected
        ? Array.AsReadOnly(new uint[] { 1, 2 }.Where(id => _connection.Capabilities.SupportsCapability(id)).ToArray())
        : Array.Empty<uint>();

    /// <inheritdoc />
    public ConnectionState State => Volatile.Read(ref _disposed) ? ConnectionState.Closed : _connection.State;

    static bool IsStartupReadinessFailure(ConnectionState state) => state is not ConnectionState.Closed;

    static bool IsTransientStartupFailure(Exception exception) => exception is
        ConnectionException or TimeoutException or IOException or SocketException or WebSocketException;

    static TimeSpan Min(TimeSpan left, TimeSpan right) => left <= right ? left : right;

    static TimeSpan ResolveDelay(TimeSpan? value, TimeSpan fallback, bool allowZero = false)
    {
        var resolved = value ?? fallback;
        if (resolved == System.Threading.Timeout.InfiniteTimeSpan)
        {
            return resolved;
        }

        if (resolved <= TimeSpan.Zero)
        {
            return allowZero ? TimeSpan.Zero : TimeSpan.FromMilliseconds(1);
        }

        return resolved;
    }

    static async Task ObserveSynchronousDisposeAsync(Task closeTask)
    {
        try
        {
            await closeTask.ConfigureAwait(false);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            // The close task's own failure is reported to its caller; this waiter only observes it.
        }
    }

    void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed))
        {
            throw new ObjectDisposedException(nameof(FitzClient), "The client is closed.");
        }
    }
}
