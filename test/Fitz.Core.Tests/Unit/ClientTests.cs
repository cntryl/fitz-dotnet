using System.Buffers;
using System.Threading.Channels;
using Cntryl.Fitz;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Observability;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ClientTests
{
    [Fact]
    public void ShouldRejectUndefinedTransportGivenClientConfigurationWhenConnectionOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        var config = new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            Transport: (ClientTransport)99);

        Assert.Throws<ArgumentOutOfRangeException>(() => new Client(config));
    }

    [Fact]
    public void ShouldRejectNullUrlGivenClientConfigurationAtPublicBoundariesWhenConnectionOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        var config = new ClientConfig(null!);

        Assert.Throws<ArgumentNullException>(() => new Client(config));
        Assert.Throws<ArgumentNullException>(() => TransportResolver.Resolve(config));
    }

    [Fact]
    public async Task ShouldBoundAllRetryAttemptsByOneOperationDeadlineGivenConfiguredClientWhenOperationRuns()
    {
        // Arrange
        await using var transport = new FakeTransport();
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Timeout: TimeSpan.FromMilliseconds(50),
                Retry: new RetryOptions(true, MaxAttempts: 100, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero)),
            () => transport);

        var started = DateTimeOffset.UtcNow;

        // Act
        var operation = connection.ExecuteWithRetryAsync(
            new RetryOperation("kv", "get", RetryClass.ReplayableRead),
            async token =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return true;
            }).AsTask();


        // Assert
        await Assert.ThrowsAsync<RequestTimeoutException>(() => operation);
        Assert.True(DateTimeOffset.UtcNow - started < TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ShouldPreserveCallerCancellationAcrossRetryDeadlineGivenConfiguredClientWhenOperationRuns()
    {
        // Arrange
        await using var transport = new FakeTransport();
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Timeout: TimeSpan.FromSeconds(1)),
            () => transport);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(25));


        // Act
        var operation = connection.ExecuteWithRetryAsync(
            new RetryOperation("kv", "get", RetryClass.ReplayableRead),
            async token =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return true;
            },
            cancellation.Token).AsTask();


        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation);
    }

    [Fact]
    public async Task ShouldCloseOnceGivenRepeatedCloseCallsWhenConnectionOperationRuns()
    {
        // Arrange
        await using var transport = new FakeTransport();
        await using var client = new Client(new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            TransportFactory: _ => transport));

        // Act
        await client.CloseAsync();
        await client.CloseAsync();

        // Assert
        Assert.Equal(ConnectionState.Closed, client.State);
    }

    [Fact]
    public void ShouldExposeTypedTransportGivenTypedClientConfigWhenConnectionOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        var config = new ClientConfig(new Uri("ws://localhost:4190/ws"), ClientTransport.WebSocket);

        Assert.Equal(ClientTransport.WebSocket, config.Transport);
    }

    [Fact]
    public void ShouldDefaultToSpecSafeTotalFrameLimitGivenConfiguredClientWhenOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        var config = new ClientConfig(new Uri("ws://localhost:4190/ws"));

        Assert.Equal(FrameCodec.MaxTransportFrameSize, config.MaxFrameSize);
    }

    [Fact]
    public void ShouldPreserveExplicitTransportGivenClientConfigWhenConnectionOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        var config = new ClientConfig(new Uri("tcp://localhost:4191"), Transport: ClientTransport.Tcp);

        Assert.Equal(ClientTransport.Tcp, config.Transport);
    }

    [Fact]
    public void ShouldDefaultMaxInFlightRequestsGivenNoConfiguredValueWhenResolved()
    {
        // Arrange
        // Act
        // Assert
        var config = new ClientConfig(new Uri("ws://localhost:4190/ws"));

        Assert.Equal(256, config.MaxInFlightRequests);
    }

    [Fact]
    public void ShouldPreserveMaxInFlightRequestsGivenClientConfigWhenConnectionOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        var config = new ClientConfig(new Uri("ws://localhost:4190/ws"), MaxInFlightRequests: 12);

        Assert.Equal(12, config.MaxInFlightRequests);
    }

    [Fact]
    public void ShouldResolveTransportGivenWebsocketAndTcpEndpointsWhenConfigured()
    {
        // Arrange
        var websocket = new ClientConfig(new Uri("ws://localhost:4190/ws"));

        // Act
        var tcp = new ClientConfig(new Uri("tcp://localhost:4191"));


        // Assert
        Assert.Equal(ClientTransport.Auto, websocket.Transport);
        Assert.Equal(ClientTransport.WebSocket, websocket.ResolvedTransportKind);
        Assert.Equal(ClientTransport.Auto, tcp.Transport);
        Assert.Equal(ClientTransport.Tcp, tcp.ResolvedTransportKind);
        Assert.Equal(1024, websocket.MaxRequestQueueSize);
        Assert.Equal(1024, websocket.ResolvedAsyncHandlers.QueueCapacity);
        Assert.True(websocket.ResolvedReconnect.Enabled);
        Assert.True(websocket.ResolvedRetry.Enabled);
        Assert.True(websocket.ResolvedHeartbeat.Enabled);
    }

    [Fact]
    public void ShouldKeepHandlerQueueCapacityIndependentGivenRequestLimitsWhenResolvingConfig()
    {
        // Arrange
        // Act
        // Assert
        var config = new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            AsyncHandlers: new AsyncHandlerOptions(MaxConcurrency: 3, QueueCapacity: 19),
            MaxRequestQueueSize: 7);

        Assert.Equal(19, config.ResolvedAsyncHandlers.QueueCapacity);
        Assert.Equal(3, config.ResolvedAsyncHandlers.MaxConcurrency);
        Assert.Equal(7, config.ResolvedMaxRequestQueueSize);
    }

    [Fact]
    public async Task ShouldAuthenticateGivenValidJwtWhenConnectFrameIsSentFirst()
    {
        // Arrange
        await using var transport = new QueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        var config = new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            AuthSettleDelay: TimeSpan.Zero,
            TransportFactory: _ => transport,
            TokenProvider: _ => ValueTask.FromResult("token-123")
        );
        await using var client = new Client(config);

        // Act
        await client.ConnectAsync();

        // Assert
        Assert.Equal(ConnectionState.Authenticated, client.State);
        Assert.True(client.IsConnected);
        Assert.Single(transport.SentFrames);
        var frame = FrameCodec.DecodeStrict(transport.SentFrames[0]);
        Assert.Equal(MessageTypes.Connect, frame.MessageType);
        Assert.Equal("token-123", System.Text.Encoding.UTF8.GetString(frame.Payload.Span));
    }

    [Fact]
    public async Task ShouldThrowOperationCanceledGivenCanceledTokenWhenConnecting()
    {
        // Arrange
        await using var transport = new FakeTransport();
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.FromSeconds(5),
                TransportFactory: _ => transport
            )
        );

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act
        var act = () => client.ConnectAsync(cts.Token);

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(act);
    }

    [Fact]
    public async Task ShouldThrowAuthenticationExceptionGivenTransportCloseDuringAuthenticationWhenConnectionOperationRuns()
    {
        // Arrange
        await using var transport = new FakeTransport(receive: _ => new ValueTask<PooledFrame>(PooledFrame.Closed));
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.FromMilliseconds(200),
                TransportFactory: _ => transport
            )
        );

        // Act
        var act = () => client.ConnectAsync();

        // Assert
        await Assert.ThrowsAsync<AuthenticationException>(act);
    }

    [Fact]
    public async Task ShouldReconnectGivenAuthenticatedTransportWhenClosedBeforeInboundFrame()
    {
        // Arrange
        await using var firstTransport = new QueuedTransport();
        await using var reconnectTransport = new QueuedTransport();
        var factoryCalls = 0;
        await using var client = new Client(new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            AuthSettleDelay: TimeSpan.Zero,
            Reconnect: new ReconnectOptions(true, MaxAttempts: 2, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero),
            TransportFactory: _ => factoryCalls++ == 0 ? firstTransport : reconnectTransport));

        await client.ConnectAsync();
        firstTransport.QueueClosed();


        // Act
        await WaitForConditionAsync(
            () => client.IsConnected && factoryCalls == 2,
            TimeSpan.FromSeconds(1));

        // Assert
        Assert.Equal(ConnectionState.Authenticated, client.State);
    }

    [Fact]
    public async Task ShouldDiscardPartialFrameGivenReconnectWhenNextSessionResponds()
    {
        // Arrange
        await using var firstTransport = new QueuedTransport();
        await using var reconnectTransport = new QueuedTransport();
        reconnectTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 2)
            {
                reconnectTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, [0, 0, 0, 0, 0, 0]));
            }
        };
        var factoryCalls = 0;
        await using var client = new Client(new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            Timeout: TimeSpan.FromMilliseconds(250),
            AuthSettleDelay: TimeSpan.Zero,
            Reconnect: new ReconnectOptions(true, MaxAttempts: 2, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero),
            TransportFactory: _ => factoryCalls++ == 0 ? firstTransport : reconnectTransport));

        await client.ConnectAsync();
        var staleFrame = FrameCodec.Encode(MessageTypes.LeaseQuery, [0]);
        firstTransport.QueueIncomingFrame(staleFrame[..2]);
        firstTransport.QueueClosed();
        await WaitForConditionAsync(() => client.IsConnected && factoryCalls == 2, TimeSpan.FromSeconds(1));


        // Act
        var result = await client.Lease.QueryAsync("lease://prod/app/lock");


        // Assert
        Assert.False(result.IsHeld);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ShouldKeepConnectionGivenOversizedCallerPayloadWhenEncoding(bool request)
    {
        // Arrange
        await using var transport = new QueuedTransport();
        var factoryCalls = 0;
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.Zero,
                Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero)),
            () =>
            {
                factoryCalls++;
                return transport;
            });
        await connection.ConnectAsync();

        var oversizedPayload = new byte[ushort.MaxValue + 1];

        // Act
        var operation = request
            ? connection.RequestAsync(42, oversizedPayload).AsTask()
            : connection.SendAsync(42, oversizedPayload).AsTask();


        // Assert
        await Assert.ThrowsAsync<ProtocolException>(() => operation);
        await Task.Delay(50);
        Assert.Equal(ConnectionState.Authenticated, connection.State);
        Assert.Equal(1, factoryCalls);
        Assert.Single(transport.SentFrames);
    }

    [Fact]
    public async Task ShouldRetryStartupTransportFailuresGivenConnectWhenReady()
    {
        // Arrange
        var attempts = 0;
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Timeout: TimeSpan.FromMilliseconds(100),
                AuthSettleDelay: TimeSpan.Zero,
                TransportFactory: _ =>
                {
                    attempts++;
                    return attempts < 2
                        ? new FailingConnectTransport(new IOException("dial failed"))
                        : new IdleTransport();
                }));


        // Act
        await client.ConnectWhenReadyAsync(new ConnectWhenReadyOptions(
            Timeout: TimeSpan.FromMilliseconds(1000),
            Backoff: TimeSpan.FromMilliseconds(1),
            MaxBackoff: TimeSpan.FromMilliseconds(1)));


        // Assert
        Assert.Equal(2, attempts);
        Assert.True(client.IsConnected);
    }


    [Fact]
    public async Task ShouldNotRetryAuthenticationGivenRejectedConnectWhenReconnectEnabled()
    {
        // Arrange
        var attempts = 0;

        // Act
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.FromMilliseconds(200),
                TransportFactory: _ =>
                {
                    attempts++;
                    return new FakeTransport(_ => new ValueTask<PooledFrame>(PooledFrame.Closed));
                }));


        // Assert
        await Assert.ThrowsAsync<AuthenticationException>(() =>
            client.ConnectWhenReadyAsync(new ConnectWhenReadyOptions(
                Timeout: TimeSpan.FromMilliseconds(250),
                Backoff: TimeSpan.FromMilliseconds(1),
                MaxBackoff: TimeSpan.FromMilliseconds(1))));

        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task ShouldTimeoutGivenConnectWhenReadyTotalDeadlineExpires()
    {
        // Arrange
        var attempts = 0;

        // Act
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                TransportFactory: _ =>
                {
                    attempts++;
                    return new FailingConnectTransport(new IOException("dial failed"));
                }));


        // Assert
        await Assert.ThrowsAsync<TimeoutException>(() =>
            client.ConnectWhenReadyAsync(new ConnectWhenReadyOptions(
                Timeout: TimeSpan.FromMilliseconds(50),
                Backoff: TimeSpan.FromMilliseconds(1),
                MaxBackoff: TimeSpan.FromMilliseconds(1))));

        Assert.True(attempts >= 1);
    }

    [Fact]
    public async Task ShouldCoalesceConcurrentConnectCallsOntoOneInflightAttemptGivenConfiguredClientWhenOperationRuns()
    {
        // Arrange
        var releaseConnect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                TransportFactory: _ =>
                {
                    attempts++;
                    return new BlockingConnectTransport(releaseConnect.Task);
                }));

        var first = client.ConnectAsync();
        var second = client.ConnectAsync();

        // Act
        await Task.Delay(25);


        // Assert
        Assert.Equal(1, attempts);

        releaseConnect.TrySetResult();
        await Task.WhenAll(first, second);
        Assert.True(client.IsConnected);
    }

    [Fact]
    public async Task ShouldStopReconnectingGivenCloseDuringBackoffWhenCloseCalled()
    {
        // Arrange
        var releaseFirstReceive = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var firstTransport = new QueuedTransport();
        firstTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        _ = Task.Run(async () =>
        {
            await releaseFirstReceive.Task;
            firstTransport.QueueClosed();
        });
        await using var secondTransport = new QueuedTransport();
        var factoryCalls = 0;

        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.Zero,
                Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(250), MaxBackoff: TimeSpan.FromMilliseconds(250)),
                TransportFactory: _ => factoryCalls++ == 0 ? firstTransport : secondTransport
            )
        );

        await client.ConnectAsync();

        releaseFirstReceive.SetResult();
        await WaitForConditionAsync(() => !client.IsConnected, TimeSpan.FromSeconds(1));


        // Act
        await client.DisposeAsync();


        // Assert
        Assert.Equal(1, factoryCalls);
        Assert.Empty(secondTransport.SentFrames);
        Assert.False(client.IsConnected);
    }

    [Fact]
    public async Task ShouldCancelAndAwaitInflightReconnectGivenCloseDuringTransportConnectWhenConnectionOperationRuns()
    {
        // Arrange
        await using var firstTransport = new QueuedTransport();
        firstTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 1)
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
            }
        };
        var neverConnects = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var reconnectTransport = new BlockingConnectTransport(neverConnects.Task);
        var factoryCalls = 0;
        await using var client = new Client(new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            AuthSettleDelay: TimeSpan.Zero,
            Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero),
            TransportFactory: _ => factoryCalls++ == 0 ? firstTransport : reconnectTransport));
        await client.ConnectAsync();
        firstTransport.QueueClosed();
        await WaitForConditionAsync(() => factoryCalls == 2, TimeSpan.FromSeconds(1));


        // Act
        await client.CloseAsync().WaitAsync(TimeSpan.FromSeconds(1));


        // Assert
        Assert.True(reconnectTransport.CancellationObserved);
        Assert.False(client.IsConnected);
    }

    [Fact]
    public async Task ShouldRestoreSessionGivenExhaustedAutomaticReconnectWhenConnectCalled()
    {
        // Arrange
        await using var firstTransport = CreateConfirmingTransport();
        await using var failedTransport = new FailingConnectTransport(new IOException("dial failed"));
        await using var restoredTransport = CreateConfirmingTransport();
        var lifecycleEvents = new List<string>();
        var factoryCalls = 0;
        await using var client = new Client(new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            AuthSettleDelay: TimeSpan.Zero,
            Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero),
            Observability: new FitzObservabilityOptions(OnLifecycleEvent: evt => lifecycleEvents.Add(evt.Event)),
            TransportFactory: _ => factoryCalls++ switch
            {
                0 => firstTransport,
                1 => failedTransport,
                _ => restoredTransport,
            }));
        await client.ConnectAsync();
        firstTransport.QueueClosed();
        await WaitForConditionAsync(
            () => lifecycleEvents.Contains("reconnect_failed", StringComparer.Ordinal),
            TimeSpan.FromSeconds(1));

        // Act
        await client.ConnectAsync();

        // Assert
        Assert.True(client.IsConnected);
        Assert.Equal(3, factoryCalls);
        Assert.Contains("reconnect_succeeded", lifecycleEvents, StringComparer.Ordinal);
    }

    [Fact]
    public async Task ShouldRetryReconnectGivenTransportClosesDuringReconnectAuthenticationWhenConnectionOperationRuns()
    {
        // Arrange
        await using var firstTransport = CreateConfirmingTransport();
        await using var interruptedReconnect = new QueuedTransport();
        interruptedReconnect.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 1)
            {
                interruptedReconnect.QueueClosed();
            }
        };
        await using var restoredTransport = CreateConfirmingTransport();
        var factoryCalls = 0;
        var lifecycleEvents = new List<string>();
        await using var client = new Client(new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            AuthSettleDelay: TimeSpan.FromMilliseconds(25),
            Reconnect: new ReconnectOptions(true, MaxAttempts: 3, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero),
            Observability: new FitzObservabilityOptions(OnLifecycleEvent: evt => lifecycleEvents.Add(evt.Event)),
            TransportFactory: _ => factoryCalls++ switch
            {
                0 => firstTransport,
                1 => interruptedReconnect,
                _ => restoredTransport,
            }));

        await client.ConnectAsync();
        firstTransport.QueueClosed();


        // Act
        await WaitForConditionAsync(
            () => client.IsConnected && factoryCalls >= 3,
            TimeSpan.FromSeconds(1));


        // Assert
        Assert.Equal(3, factoryCalls);
        Assert.Contains("reconnect_succeeded", lifecycleEvents, StringComparer.Ordinal);
    }

    [Fact]
    public async Task ShouldRetryReconnectAndInvokeAllListenersGivenRestoreFailureWhenConnectionOperationRuns()
    {
        // Arrange
        await using var firstTransport = CreateConfirmingTransport();
        await using var failedRestoreTransport = CreateConfirmingTransport();
        await using var restoredTransport = CreateConfirmingTransport();
        var factoryCalls = 0;
        var failingListenerCalls = 0;
        var remainingListenerCalls = 0;
        var reconnectFailures = 0;
        var reconnectSuccesses = 0;
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.Zero,
                Reconnect: new ReconnectOptions(true, MaxAttempts: 3, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero),
                Observability: new FitzObservabilityOptions(OnLifecycleEvent: evt =>
                {
                    if (evt.Event == "reconnect_failed")
                    {
                        Interlocked.Increment(ref reconnectFailures);
                    }
                    else if (evt.Event == "reconnect_succeeded")
                    {
                        Interlocked.Increment(ref reconnectSuccesses);
                    }
                })),
            () => factoryCalls++ switch
            {
                0 => firstTransport,
                1 => failedRestoreTransport,
                _ => restoredTransport,
            });
        using var failingRegistration = connection.OnReconnect(_ =>
        {
            failingListenerCalls++;
            if (failingListenerCalls == 1)
            {
                throw new InvalidOperationException("restore failed");
            }

            return ValueTask.CompletedTask;
        });
        using var remainingRegistration = connection.OnReconnect(_ =>
        {
            remainingListenerCalls++;
            return ValueTask.CompletedTask;
        });

        await connection.ConnectAsync();
        firstTransport.QueueClosed();


        // Act
        await WaitForConditionAsync(
            () => connection.State == ConnectionState.Authenticated
                && factoryCalls >= 3
                && Volatile.Read(ref reconnectSuccesses) == 1,
            TimeSpan.FromSeconds(1));


        // Assert
        Assert.Equal(3, factoryCalls);
        Assert.Equal(2, failingListenerCalls);
        Assert.Equal(2, remainingListenerCalls);
        Assert.Equal(1, Volatile.Read(ref reconnectFailures));
        Assert.Equal(1, Volatile.Read(ref reconnectSuccesses));
    }

    [Fact]
    public async Task ShouldWithholdAuthenticatedStateGivenRestorationInProgressWhenReconnectRuns()
    {
        // Arrange
        await using var firstTransport = CreateConfirmingTransport();
        await using var reconnectTransport = CreateConfirmingTransport();
        var restoreStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRestore = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var factoryCalls = 0;
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.Zero,
                Reconnect: new ReconnectOptions(true, MaxAttempts: 2, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero)),
            () => factoryCalls++ == 0 ? firstTransport : reconnectTransport);
        using var registration = connection.OnReconnect(async cancellationToken =>
        {
            restoreStarted.TrySetResult();
            await releaseRestore.Task.WaitAsync(cancellationToken);
        });

        await connection.ConnectAsync();
        firstTransport.QueueClosed();

        // Act
        await restoreStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));


        // Assert
        Assert.Equal(ConnectionState.Reconnecting, connection.State);

        releaseRestore.TrySetResult();
        await WaitForConditionAsync(
            () => connection.State == ConnectionState.Authenticated,
            TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ShouldWaitForCancellationWithoutRetryingGivenInfiniteBackoffWhenExecutingRetryPolicy()
    {
        // Arrange
        await using var transport = new FakeTransport();
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Timeout: TimeSpan.FromSeconds(5),
                Retry: new RetryOptions(
                    true,
                    MaxAttempts: int.MaxValue,
                    Backoff: Timeout.InfiniteTimeSpan,
                    MaxBackoff: Timeout.InfiniteTimeSpan)),
            () => transport);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var attempts = 0;


        // Act
        var operation = connection.ExecuteWithRetryAsync(
            new RetryOperation("kv", "get", RetryClass.ReplayableRead),
            _ =>
            {
                Interlocked.Increment(ref attempts);
                return ValueTask.FromException<bool>(new ConnectionException("retry"));
            },
            cancellation.Token).AsTask();


        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation);
        Assert.Equal(1, Volatile.Read(ref attempts));
    }

    [Fact]
    public async Task ShouldResetResponseLaneGivenSentRequestWhenOuterDeadlineExpires()
    {
        // Arrange
        await using var transport = CreateConfirmingTransport();
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Timeout: TimeSpan.FromMilliseconds(50),
                AuthSettleDelay: TimeSpan.Zero,
                Retry: new RetryOptions(true, MaxAttempts: 1, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero)),
            () => transport);
        await connection.ConnectAsync();


        // Act
        var operation = connection.ExecuteWithRetryAsync(
            new RetryOperation("kv", "get", RetryClass.ReplayableRead),
            cancellationToken => connection.RequestAsync(88, ReadOnlyMemory<byte>.Empty, cancellationToken)).AsTask();


        // Assert
        await Assert.ThrowsAsync<RequestTimeoutException>(() => operation);
        await WaitForConditionAsync(
            () => connection.State != ConnectionState.Authenticated,
            TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ShouldIsolateBackpressureGivenOneSaturatedDomainWhenDispatchingAnotherDomain()
    {
        // Arrange
        await using var transport = new FakeTransport();
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AsyncHandlers: new AsyncHandlerOptions(
                    MaxConcurrency: 1,
                    Timeout: Timeout.InfiniteTimeSpan,
                    QueueCapacity: 0),
                TransportFactory: _ => transport),
            () => transport);
        var noticeStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseNotice = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // Act
        var queueCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);


        // Assert
        Assert.True(connection.TryDispatchAsyncHandler("notice", async cancellationToken =>
        {
            noticeStarted.TrySetResult();
            await releaseNotice.Task.WaitAsync(cancellationToken);
        }));
        await noticeStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.True(connection.TryDispatchAsyncHandler("queue", _ =>
        {
            queueCompleted.TrySetResult();
            return ValueTask.CompletedTask;
        }));
        await queueCompleted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        releaseNotice.TrySetResult();
    }

    [Fact]
    public async Task ShouldUseInfiniteTimeoutGivenDefaultHandlerOptionsWhenDispatchingAsyncHandler()
    {
        // Arrange
        await using var transport = new IdleTransport();
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Timeout: TimeSpan.FromMilliseconds(20),
                AsyncHandlers: new AsyncHandlerOptions(MaxConcurrency: 1),
                TransportFactory: _ => transport),
            () => transport);

        // Act
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);


        // Assert
        Assert.True(connection.TryDispatchAsyncHandler("notice", async cancellationToken =>
        {
            await Task.Delay(75, CancellationToken.None);
            completed.TrySetResult(cancellationToken.IsCancellationRequested);
        }));

        Assert.False(await completed.Task.WaitAsync(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void ShouldAcceptFiniteBackoffGivenInfiniteMaximumWhenValidatingClientConfig()
    {
        // Arrange
        // Act
        // Assert
        using var client = new Client(new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            Retry: new RetryOptions(
                Backoff: TimeSpan.FromMilliseconds(100),
                MaxBackoff: Timeout.InfiniteTimeSpan),
            TransportFactory: _ => new FakeTransport()));

        Assert.Equal(Timeout.InfiniteTimeSpan, client.Config.ResolvedRetry.MaxBackoff);
    }

    [Fact]
    public async Task ShouldSurfaceRequestFailureWithoutWaitingForBackgroundReconnectGivenConfiguredClientWhenOperationRuns()
    {
        // Arrange
        await using var firstTransport = new QueuedTransport();
        firstTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 1)
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
                return;
            }

            throw new IOException("send failed");
        };
        var reconnectNeverCompletes = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var reconnectTransport = new BlockingConnectTransport(reconnectNeverCompletes.Task);
        var factoryCalls = 0;
        await using var client = new Client(new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            Timeout: TimeSpan.FromMilliseconds(100),
            AuthSettleDelay: TimeSpan.Zero,
            Reconnect: new ReconnectOptions(true, int.MaxValue, TimeSpan.Zero, TimeSpan.Zero),
            TransportFactory: _ => factoryCalls++ == 0 ? firstTransport : reconnectTransport));
        await client.ConnectAsync();


        // Act
        var request = client.Kv.BeginAsync(
            "kv://prod/app/users",
            Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);


        // Assert
        var exception = await Assert.ThrowsAsync<ConnectionException>(
            () => request.WaitAsync(TimeSpan.FromMilliseconds(500)));
        Assert.Contains("send failed", exception.Message, StringComparison.Ordinal);
        await WaitForConditionAsync(() => factoryCalls >= 2, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ShouldResetConnectionGivenUncorrelatedRequestTimeoutWhenConnectionOperationRuns()
    {
        // Arrange
        await using var transport = new QueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Timeout: TimeSpan.FromMilliseconds(50),
                AuthSettleDelay: TimeSpan.Zero,
                TransportFactory: _ => transport
            )
        );


        // Act
        await client.ConnectAsync();


        // Assert
        var ex = await Assert.ThrowsAsync<RequestTimeoutException>(() =>
            client.Kv.BeginAsync("kv://prod/app/users", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async));

        Assert.Contains("Request timeout", ex.Message, StringComparison.OrdinalIgnoreCase);
        await WaitForConditionAsync(() => !client.IsConnected, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ShouldBoundConcurrentOutboundRequestsGivenMaxOneWhenSecondRequestStarts()
    {
        // Arrange
        await using var transport = new QueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.Zero,
                MaxInFlightRequests: 1,
                TokenProvider: _ => ValueTask.FromResult("token-123")
            ),
            () => transport
        );

        await connection.ConnectAsync();

        var firstRequest = connection.RequestAsync(77, "first"u8.ToArray());
        await WaitForConditionAsync(() => transport.SentFrames.Count == 2, TimeSpan.FromSeconds(1));

        var secondRequest = connection.RequestAsync(77, "second"u8.ToArray());


        // Act
        await Task.Delay(50);

        // Assert
        Assert.False(secondRequest.IsCompleted);
        Assert.Equal(2, transport.SentFrames.Count);

        transport.QueueIncomingFrame(FrameCodec.Encode(77, "ok"u8));
        Assert.Equal("ok", System.Text.Encoding.UTF8.GetString((await firstRequest).Span));

        await WaitForConditionAsync(() => transport.SentFrames.Count == 3, TimeSpan.FromSeconds(1));
        transport.QueueIncomingFrame(FrameCodec.Encode(77, "done"u8));
        Assert.Equal("done", System.Text.Encoding.UTF8.GetString((await secondRequest).Span));

        await connection.CloseAsync();
    }

    [Fact]
    public async Task ShouldThrowRequestQueueFullGivenWaiterLimitReachedWhenConnectionOperationRuns()
    {
        // Arrange
        await using var transport = new QueuedTransport();

        // Act
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.Zero,
                MaxInFlightRequests: 1,
                MaxRequestQueueSize: 1),
            () => transport);

        // Assert
        try
        {
            await connection.ConnectAsync();

            var first = connection.RequestAsync(88, "first"u8.ToArray());
            await WaitForConditionAsync(() => transport.SentFrames.Count == 2, TimeSpan.FromSeconds(1));

            var second = connection.RequestAsync(88, "second"u8.ToArray());
            await Task.Delay(25);
            Assert.False(second.IsCompleted);

            var ex = await Assert.ThrowsAsync<RequestQueueFullException>(() =>
                connection.RequestAsync(88, "third"u8.ToArray()).AsTask());

            Assert.Contains("queue", ex.Message, StringComparison.OrdinalIgnoreCase);

            transport.QueueIncomingFrame(FrameCodec.Encode(88, "ok"u8));
            await WaitForConditionAsync(() => transport.SentFrames.Count == 3, TimeSpan.FromSeconds(1));
            transport.QueueIncomingFrame(FrameCodec.Encode(88, "done"u8));
            await Task.WhenAll(first.AsTask(), second.AsTask());
        }
        finally
        {
            await connection.CloseAsync();
        }
    }

    [Fact]
    public async Task ShouldReceiveNoticeNotificationGivenConnectionBackedSubscriptionWhenConnectionOperationRuns()
    {
        // Arrange
        await using var transport = new QueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.Zero,
                TransportFactory: _ => transport));

        await client.ConnectAsync();

        var subscribeTask = client.Notice.SubscribeAsync("notice://prod/app/*");

        using (var subscribeResponse = new BinaryBufferWriter())
        {
            subscribeResponse.WriteU8(0);
            subscribeResponse.WriteU8(1);
            subscribeResponse.WriteU64(55);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.NoticeSubscribe, subscribeResponse.WrittenSpan));
        }

        var subscription = await subscribeTask;
        var received = ReadFirstAsync(subscription);
        const ulong subscriptionId = 55;

        using (var notification = new BinaryBufferWriter())
        {
            notification.WriteU64(subscriptionId);
            notification.WriteString("notice://prod/app/events");
            notification.WriteU32(5);
            notification.WriteBytes("hello"u8);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.NoticeNotify, notification.WrittenSpan));
        }

        var message = await received.WaitAsync(TimeSpan.FromSeconds(1));

        // Act
        var result = (message.Route, Body: message.Body.ToArray());


        // Assert
        Assert.Equal("notice://prod/app/events", result.Route);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(result.Body));

        var disposeTask = subscription.DisposeAsync().AsTask();
        using (var unsubscribeResponse = new BinaryBufferWriter())
        {
            unsubscribeResponse.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(
                MessageTypes.NoticeUnsubscribe,
                unsubscribeResponse.WrittenSpan));
        }
        await disposeTask;
    }

    static async Task<T> ReadFirstAsync<T>(IAsyncEnumerable<T> notifications)
    {
        await foreach (var notification in notifications)
        {
            return notification;
        }

        throw new InvalidOperationException("Subscription completed before a notification arrived");
    }

    static async Task WaitForConditionAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;

        while (!condition())
        {
            if (DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out waiting for the requested condition.");
            }

            await Task.Delay(10);
        }
    }

    sealed class FakeTransport : ITransport
    {
        readonly Func<CancellationToken, ValueTask<PooledFrame>> _receive;

        public FakeTransport(Func<CancellationToken, ValueTask<PooledFrame>>? receive = null)
        {
            _receive = receive ?? (_ => new ValueTask<PooledFrame>(PooledFrame.Closed));
        }

        public List<byte[]> SentFrames { get; } = [];

        public Uri Url { get; } = new("ws://fake");

        public Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            SentFrames.Add(data.ToArray());
            return Task.CompletedTask;
        }

        public ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return _receive(cancellationToken);
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    sealed class FailingConnectTransport : ITransport
    {
        readonly Exception _exception;

        public FailingConnectTransport(Exception exception)
        {
            _exception = exception;
        }

        public Uri Url { get; } = new("ws://failing");

        public Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromException(_exception);
        }

        public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<PooledFrame>(PooledFrame.Closed);
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    sealed class IdleTransport : ITransport
    {
        public Uri Url { get; } = new("ws://idle");

        public Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, cancellationToken);
            return PooledFrame.Closed;
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    sealed class BlockingConnectTransport : ITransport
    {
        readonly Task _connectSignal;

        public BlockingConnectTransport(Task connectSignal)
        {
            _connectSignal = connectSignal;
        }

        public Uri Url { get; } = new("ws://blocking");

        public bool CancellationObserved { get; private set; }

        public async Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await _connectSignal.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                CancellationObserved = true;
                throw;
            }
        }

        public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, cancellationToken);
            return PooledFrame.Closed;
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    sealed class QueuedTransport : ITransport
    {
        readonly Channel<PooledFrame> _incoming = Channel.CreateUnbounded<PooledFrame>();
        readonly object _sentFramesGate = new();

        public List<byte[]> SentFrames { get; } = [];
        public Action<int>? AfterSend { get; set; }

        public Uri Url { get; } = new("ws://queued");

        public Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int sentFrameCount;
            lock (_sentFramesGate)
            {
                SentFrames.Add(data.ToArray());
                sentFrameCount = SentFrames.Count;
            }

            AfterSend?.Invoke(sentFrameCount);

            return Task.CompletedTask;
        }

        public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await _incoming.Reader.ReadAsync(cancellationToken);
        }

        public void QueueIncomingFrame(byte[] frame)
        {
            ArgumentNullException.ThrowIfNull(frame);

            var buffer = ArrayPool<byte>.Shared.Rent(frame.Length);
            frame.AsSpan().CopyTo(buffer);
            _incoming.Writer.TryWrite(PooledFrame.FromRentedBuffer(buffer, frame.Length));
        }

        public void QueueClosed() => _incoming.Writer.TryWrite(PooledFrame.Closed);

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _incoming.Writer.TryComplete();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            _incoming.Writer.TryComplete();
            return ValueTask.CompletedTask;
        }
    }

    static QueuedTransport CreateConfirmingTransport()
    {
        var transport = new QueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        return transport;
    }
}
