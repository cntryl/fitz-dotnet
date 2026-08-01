using Cntryl.Fitz.Abstractions;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Domains.Notice;
using Cntryl.Fitz.Domains.Queue;
using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Domains.Schedule;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Transport;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz;

public sealed class Client : IClient
{
    private readonly ClientConfig _config;
    private readonly FitzConnection _connection;
    private KvClient? _kvClient;
    private LeaseClient? _leaseClient;
    private NoticeClient? _noticeClient;
    private QueueClient? _queueClient;
    private RpcClient? _rpcClient;
    private ScheduleClient? _scheduleClient;
    private StreamClient? _streamClient;
    private bool _disposed;

    public Client(ClientConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        var transportFactory = _config.TransportFactory ?? TransportResolver.Resolve;
        _connection = new FitzConnection(_config, () => transportFactory(_config));
    }

    public ClientConfig Config => _config;

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return _connection.ConnectAsync(cancellationToken);
    }

    public async Task ConnectWhenReadyAsync(ConnectWhenReadyOptions? options = null, CancellationToken cancellationToken = default)
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
            cancellationToken.ThrowIfCancellationRequested();
            attempts++;

            var remaining = deadline - DateTimeOffset.UtcNow;
            if (timeout == TimeSpan.Zero && attempts > 1)
            {
                throw new TimeoutException("Timed out waiting for Fitz to become ready.");
            }

            using var attemptCts = timeout == System.Threading.Timeout.InfiniteTimeSpan
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
                : CreateAttemptCancellationSource(remaining, cancellationToken);

            try
            {
                await ConnectAsync(attemptCts.Token).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && timeout != System.Threading.Timeout.InfiniteTimeSpan && DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out waiting for Fitz to become ready.");
            }
            catch (AuthenticationException)
            {
                throw;
            }
            catch (ConnectionException) when (_disposed || State == ConnectionState.Closed)
            {
                throw;
            }
            catch (Exception) when (timeout != System.Threading.Timeout.InfiniteTimeSpan && DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out waiting for Fitz to become ready.");
            }
            catch (Exception) when (IsStartupReadinessFailure(State))
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
                    await Task.Delay(actualDelay, cancellationToken).ConfigureAwait(false);
                }

                backoff = Min(TimeSpan.FromMilliseconds(backoff.TotalMilliseconds * 2), maxBackoff);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _kvClient?.Dispose();
        _leaseClient?.Dispose();
        _noticeClient?.Dispose();
        _queueClient?.Dispose();
        _scheduleClient?.Dispose();
        _streamClient?.Dispose();
        await _connection.DisposeAsync().ConfigureAwait(false);
    }

    public IKvClient Kv()
    {
        return _kvClient ??= new KvClient(_connection);
    }

    public ILeaseClient Lease()
    {
        return _leaseClient ??= new LeaseClient(_connection);
    }

    public INoticeClient Notice()
    {
        return _noticeClient ??= new NoticeClient(_connection);
    }

    public IQueueClient Queue()
    {
        return _queueClient ??= new QueueClient(_connection);
    }

    public IRpcClient Rpc()
    {
        return _rpcClient ??= new RpcClient(_connection);
    }

    public IScheduleClient Schedule()
    {
        return _scheduleClient ??= new ScheduleClient(_connection);
    }

    public IStreamClient Stream()
    {
        return _streamClient ??= new StreamClient(_connection);
    }

    public bool IsConnected => _connection.State == ConnectionState.Authenticated;

    public ConnectionState State => _disposed ? ConnectionState.Closed : _connection.State;

    private static CancellationTokenSource CreateAttemptCancellationSource(TimeSpan remaining, CancellationToken cancellationToken)
    {
        var timeout = remaining <= TimeSpan.Zero ? TimeSpan.Zero : remaining;
        var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        source.CancelAfter(timeout);
        return source;
    }

    private static bool IsStartupReadinessFailure(ConnectionState state)
    {
        return state is not ConnectionState.Closed;
    }

    private static TimeSpan Min(TimeSpan left, TimeSpan right)
    {
        return left <= right ? left : right;
    }

    private static TimeSpan ResolveDelay(TimeSpan? value, TimeSpan fallback, bool allowZero = false)
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

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ConnectionException("Client is closed.");
        }
    }
}
