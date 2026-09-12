using System.Diagnostics.CodeAnalysis;
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
/// The default <see cref="IClient"/>: owns one broker connection and every domain API.
/// </summary>
/// <remarks>
/// Safe for concurrent use. Domain clients are created on first access and share the
/// connection. Prefer <c>await using</c>; synchronous <see cref="Dispose"/> starts the
/// asynchronous close without blocking on it.
/// </remarks>
public sealed class Client : IClient, IDisposable
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
    int _disposed;

    /// <summary>
    /// Creates a client. No connection is opened until you call a connect method.
    /// </summary>
    /// <param name="config">Configuration for the connection. Validated immediately.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is <see langword="null"/>.</exception>
    public Client(ClientConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        var transportFactory = _config.TransportFactory ?? TransportResolver.Resolve;
        _connection = new FitzConnection(_config, () => transportFactory(_config));
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
        ThrowIfDisposed();

        options ??= new ConnectWhenReadyOptions();
        var timeout = ResolveDelay(options.Timeout, _config.Timeout ?? TimeSpan.FromSeconds(30), allowZero: true);
        var backoff = ResolveDelay(options.Backoff, TimeSpan.FromMilliseconds(250));
        var maxBackoff = ResolveDelay(options.MaxBackoff, TimeSpan.FromSeconds(2));
        var deadline = DateTimeOffset.UtcNow + timeout;
        var attempts = 0;

        while (true)
        {
            ThrowIfDisposed();
            ct.ThrowIfCancellationRequested();
            attempts++;

            var remaining = deadline - DateTimeOffset.UtcNow;
            if (timeout == TimeSpan.Zero && attempts > 1)
            {
                throw new TimeoutException("Timed out waiting for Fitz to become ready.");
            }

            using var attemptCts = timeout == System.Threading.Timeout.InfiniteTimeSpan
                ? CancellationTokenSource.CreateLinkedTokenSource(ct)
                : CreateAttemptCancellationSource(remaining, ct);

            try
            {
                await ConnectAsync(attemptCts.Token).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException cancellation) when (!ct.IsCancellationRequested && timeout != System.Threading.Timeout.InfiniteTimeSpan && DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out waiting for Fitz to become ready.", cancellation);
            }
            catch (AuthenticationException)
            {
                throw;
            }
            catch (ConnectionException) when (Volatile.Read(ref _disposed) != 0 || State == ConnectionState.Closed)
            {
                throw;
            }
            catch (Exception attemptFailure) when (timeout != System.Threading.Timeout.InfiniteTimeSpan && DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out waiting for Fitz to become ready.", attemptFailure);
            }
            catch (Exception exception) when (IsStartupReadinessFailure(State) && IsTransientStartupFailure(exception))
            {
                var remainingDelay = timeout == System.Threading.Timeout.InfiniteTimeSpan
                    ? maxBackoff
                    : deadline - DateTimeOffset.UtcNow;
                if (timeout != System.Threading.Timeout.InfiniteTimeSpan && remainingDelay <= TimeSpan.Zero)
                {
                    throw new TimeoutException("Timed out waiting for Fitz to become ready.");
                }

                var actualDelay = timeout == System.Threading.Timeout.InfiniteTimeSpan
                    ? backoff
                    : Min(backoff, remainingDelay);
                if (actualDelay > TimeSpan.Zero)
                {
                    await Task.Delay(actualDelay, ct).ConfigureAwait(false);
                }

                backoff = Min(TimeSpan.FromMilliseconds(backoff.TotalMilliseconds * 2), maxBackoff);
            }
        }
    }

    /// <inheritdoc />
    public async Task CloseAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
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
                _kvClient.Value.Dispose();
            if (_leaseClient.IsValueCreated)
                _leaseClient.Value.Dispose();
            if (_noticeClient.IsValueCreated)
                _noticeClient.Value.Dispose();
            if (_queueClient.IsValueCreated)
                _queueClient.Value.Dispose();
            if (_rpcClient.IsValueCreated)
                _rpcClient.Value.Dispose();
            if (_scheduleClient.IsValueCreated)
                _scheduleClient.Value.Dispose();
            if (_streamClient.IsValueCreated)
                _streamClient.Value.Dispose();
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

    /// <inheritdoc />
    public ConnectionState State => Volatile.Read(ref _disposed) != 0 ? ConnectionState.Closed : _connection.State;

    static CancellationTokenSource CreateAttemptCancellationSource(TimeSpan remaining, CancellationToken ct)
    {
        var timeout = remaining <= TimeSpan.Zero ? TimeSpan.Zero : remaining;
        var source = CancellationTokenSource.CreateLinkedTokenSource(ct);
        source.CancelAfter(timeout);
        return source;
    }

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

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Synchronous DI disposal starts asynchronous cleanup without surfacing an unobserved background exception.")]
    static async Task ObserveSynchronousDisposeAsync(Task closeTask)
    {
        try
        {
            await closeTask.ConfigureAwait(false);
        }
        catch
        {
        }
    }

    void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ConnectionException("Client is closed.");
        }
    }
}
