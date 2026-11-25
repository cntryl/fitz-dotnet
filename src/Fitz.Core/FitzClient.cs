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
/// connection. Use <c>await using</c> to await shutdown.
/// </remarks>
public sealed class FitzClient : IFitzClient
{
    readonly ClientConfig _config;
    readonly FitzConnection _connection;
    KvClient? _kvClient;
    LeaseClient? _leaseClient;
    NoticeClient? _noticeClient;
    QueueClient? _queueClient;
    RpcClient? _rpcClient;
    ScheduleClient? _scheduleClient;
    StreamClient? _streamClient;
    readonly Lock _lifecycleGate = new();
    Task? _shutdown;

    /// <summary>
    /// Creates a client. No connection is opened until you call a connect method.
    /// </summary>
    /// <param name="config">Configuration for the connection. Validated immediately.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is <see langword="null"/>.</exception>
    public FitzClient(ClientConfig config)
        : this(config, TimeProvider.System)
    {
    }

    /// <summary>Creates a client with an application-supplied clock.</summary>
    /// <param name="config">Configuration for the connection. Validated immediately.</param>
    /// <param name="timeProvider">The clock for client deadlines, retries, and built-in transport timeouts.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    public FitzClient(ClientConfig config, TimeProvider timeProvider)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        ArgumentNullException.ThrowIfNull(timeProvider);
        var transportFactory = _config.TransportFactory;
        _connection = new FitzConnection(
            _config,
            () => transportFactory is null ? TransportResolver.Resolve(_config, timeProvider) : transportFactory(_config),
            timeProvider);
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
            catch (ConnectionException) when (State == ConnectionState.Closed)
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
    public Task CloseAsync(CancellationToken ct = default)
    {
        TaskCompletionSource<Task> start;
        Task shutdown;
        lock (_lifecycleGate)
        {
            if (_shutdown is { } existing)
                return existing.WaitAsync(ct);

            start = new TaskCompletionSource<Task>(TaskCreationOptions.RunContinuationsAsynchronously);
            shutdown = start.Task.Unwrap();
            Volatile.Write(ref _shutdown, shutdown);
        }

        // Publish the completion task before running any session cleanup callbacks.
        start.SetResult(CloseCoreAsync());
        return shutdown.WaitAsync(ct);
    }

    async Task CloseCoreAsync()
    {
        try
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            if (_kvClient is { } kv)
                await kv.DisposeAsync().ConfigureAwait(false);

            if (_leaseClient is { } lease)
                await lease.DisposeAsync().ConfigureAwait(false);

            if (_noticeClient is { } notice)
                await notice.DisposeAsync().ConfigureAwait(false);

            if (_queueClient is { } queue)
                await queue.DisposeAsync().ConfigureAwait(false);

            if (_rpcClient is { } rpc)
                rpc.Dispose();

            if (_scheduleClient is { } schedule)
                await schedule.DisposeAsync().ConfigureAwait(false);

            if (_streamClient is { } stream)
                await stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Closes the connection and releases everything it owns.</summary>
    /// <returns>A task that completes once the client is closed.</returns>
    public async ValueTask DisposeAsync() => await CloseAsync().ConfigureAwait(false);

    /// <inheritdoc />
    public IKvClient Kv => GetDomain(ref _kvClient, static connection => new KvClient(connection));

    /// <inheritdoc />
    public ILeaseClient Lease => GetDomain(ref _leaseClient, static connection => new LeaseClient(connection));

    /// <inheritdoc />
    public INoticeClient Notice => GetDomain(ref _noticeClient, static connection => new NoticeClient(connection));

    /// <inheritdoc />
    public IQueueClient Queue => GetDomain(ref _queueClient, static connection => new QueueClient(connection));

    /// <inheritdoc />
    public IRpcClient Rpc => GetDomain(ref _rpcClient, static connection => new RpcClient(connection));

    /// <inheritdoc />
    public IScheduleClient Schedule => GetDomain(ref _scheduleClient, static connection => new ScheduleClient(connection));

    /// <inheritdoc />
    public IStreamClient Stream => GetDomain(ref _streamClient, static connection => new StreamClient(connection));

    /// <inheritdoc />
    public bool IsConnected => State == ConnectionState.Authenticated;

    /// <summary>Numeric capabilities negotiated for the current live session.</summary>
    public IReadOnlyList<uint> CapabilityIds => IsConnected
        ? Array.AsReadOnly(new uint[] { 1, 2 }.Where(id => _connection.Capabilities.SupportsCapability(id)).ToArray())
        : Array.Empty<uint>();

    /// <inheritdoc />
    public ConnectionState State => Volatile.Read(ref _shutdown) is not null ? ConnectionState.Closed : _connection.State;

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

    T GetDomain<T>(ref T? domain, Func<FitzConnection, T> create) where T : class
    {
        lock (_lifecycleGate)
        {
            ThrowIfDisposed();
            return domain ??= create(_connection);
        }
    }

    void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _shutdown) is not null)
        {
            throw new ObjectDisposedException(nameof(FitzClient), "The client is closed.");
        }
    }
}
