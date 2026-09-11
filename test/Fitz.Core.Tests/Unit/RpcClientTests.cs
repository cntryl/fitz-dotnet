using System.Runtime.CompilerServices;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RpcClientTests
{
    [Fact]
    public async Task ShouldRejectNullHandlerGivenWorkerRegistrationWhenBeforeTransport()
    {
        // Arrange
        var requestCalled = false;

        // Act
        using var rpc = new RpcClient(
            (_, _, _) =>
            {
                requestCalled = true;
                return Task.FromResult(Array.Empty<byte>());
            },
            registerNotificationHandler: (_, _) => new TestRegistration());


        // Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => rpc.RegisterWorkerAsync(
            "rpc://prod/app/*",
            null!));

        Assert.False(requestCalled);
    }

    [Fact]
    public async Task ShouldReturnTypedErrorGivenEmptyResponseWhenRegisteringWorker()
    {
        // Arrange
        // Act
        // Assert
        using var rpc = new RpcClient(
            (_, _, _) => Task.FromResult(Array.Empty<byte>()),
            registerNotificationHandler: (_, _) => new TestRegistration());

        var error = await Assert.ThrowsAsync<RpcException>(() => rpc.RegisterWorkerAsync(
            "rpc://prod/app/*",
            (_, _, _) => ValueTask.CompletedTask));

        Assert.Equal("REGISTER_INVALID_RESPONSE", error.Code);
    }

    [Fact]
    public async Task ShouldRegisterOneResponseHandlerAcrossMultipleCallsGivenRpcClientWhenOperationRuns()
    {
        // Arrange
        Action<byte[]>? responseHandler = null;

        // Act
        var responseRegistrations = 0;

        // Assert
        using var rpc = new RpcClient(
            (_, payload, _) =>
            {
                var request = new BinaryBufferReader(payload);
                var correlationId = request.ReadBytes(16);
                using var response = new BinaryBufferWriter();
                response.WriteBytes(correlationId);
                response.WriteU64(0);
                response.WriteU8(1);
                response.WriteU32(0);
                responseHandler!(response.Build());
                return Task.FromResult(Array.Empty<byte>());
            },
            registerNotificationHandler: (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.RpcResponse, messageType);
                responseRegistrations++;
                responseHandler = handler;
                return new TestRegistration();
            });

        await DrainAsync(rpc.CallAsync("rpc://prod/app/one", ReadOnlyMemory<byte>.Empty));
        await DrainAsync(rpc.CallAsync("rpc://prod/app/two", ReadOnlyMemory<byte>.Empty));

        Assert.Equal(1, responseRegistrations);

        static async Task DrainAsync(IAsyncEnumerable<RpcResponseFrame> responses)
        {
            await foreach (var _ in responses)
            {
            }
        }
    }

    [Fact]
    public void ShouldValidateRouteBeforeRpcCallIsEnumeratedGivenRpcClientWhenOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        using var rpc = new RpcClient((_, _, _) => Task.FromResult(Array.Empty<byte>()));

        var exception = Assert.Throws<RpcException>(() =>
            rpc.CallAsync("not-a-route", ReadOnlyMemory<byte>.Empty));

        Assert.Equal("INVALID_ROUTE", exception.Code);
    }

    [Fact]
    public async Task ShouldRegisterRequestHandlerOnceGivenConcurrentWorkerRegistrationWhenRpcOperationRuns()
    {
        // Arrange
        var bothRequestsStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseResponses = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var requestCount = 0;

        // Act
        var handlerRegistrations = 0;

        // Assert
        using var rpc = new RpcClient(
            async (_, _, _) =>
            {
                if (Interlocked.Increment(ref requestCount) == 2)
                {
                    bothRequestsStarted.TrySetResult();
                }
                await releaseResponses.Task;
                return new byte[] { 0, 0, 0, 0, 0 };
            },
            registerNotificationHandler: (messageType, _) =>
            {
                Assert.Equal(MessageTypes.RpcRequest, messageType);
                Interlocked.Increment(ref handlerRegistrations);
                return new TestRegistration();
            });

        var first = rpc.RegisterWorkerAsync("rpc://prod/app/one", (_, _, _) => ValueTask.CompletedTask);
        var second = rpc.RegisterWorkerAsync("rpc://prod/app/two", (_, _, _) => ValueTask.CompletedTask);
        await bothRequestsStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
        releaseResponses.TrySetResult();

        await using var firstRegistration = await first;
        await using var secondRegistration = await second;
        Assert.Equal(1, handlerRegistrations);
    }

    [Fact]
    public async Task ShouldSendRpcRequestAndYieldResponseFramesGivenValidStreamWhenInvokingRpc()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;
        Action<byte[]>? responseHandler = null;

        using var rpc = new RpcClient(
            (messageType, payload, _) =>
            {
                seenMessageType = messageType;
                seenPayload = payload;
                return Task.FromResult(Array.Empty<byte>());
            },
            registerNotificationHandler: (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.RpcResponse, messageType);
                responseHandler = handler;
                return new TestRegistration();
            });

        // Act
        var frames = new List<RpcResponseFrame>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var task = Task.Run(async () =>
        {
            await foreach (var _ in rpc.CallAsync("rpc://prod/app/echo", new ReadOnlyMemory<byte>("ping"u8.ToArray()), cts.Token))
            {
                frames.Add(_);
            }
        });

        await Task.Delay(25);
        Assert.NotNull(responseHandler);
        using var notification = new BinaryBufferWriter();
        var requestReader = new BinaryBufferReader(seenPayload!);
        var correlationId = requestReader.ReadBytes(16);
        notification.WriteBytes(correlationId);
        notification.WriteU64(0);
        notification.WriteU8(1);
        notification.WriteU32(4);
        notification.WriteBytes("pong"u8);
        responseHandler!(notification.Build());

        await task;

        // Assert
        Assert.Equal(MessageTypes.RpcRequest, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Single(frames);
        Assert.Equal("pong", System.Text.Encoding.UTF8.GetString(frames[0].Body.Span));

        var reader = new BinaryBufferReader(seenPayload!);
        _ = reader.ReadBytes(16);
        Assert.Equal("rpc://prod/app/echo", reader.ReadString());
        Assert.Equal((uint)4, reader.ReadU32());
        Assert.Equal("ping", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(4)));
        Assert.True(reader.IsEof);
    }

    [Fact]
    public async Task ShouldThrowRpcExceptionGivenTerminalErrorResponseWhenCallingRpc()
    {
        // Arrange
        // Act
        Action<byte[]>? responseHandler = null;
        using var rpc = new RpcClient(
            (_, payload, _) =>
            {
                var reader = new BinaryBufferReader(payload);
                var correlationId = reader.ReadBytes(16);

                using var notification = new BinaryBufferWriter();
                notification.WriteBytes(correlationId);
                notification.WriteU64(0);
                notification.WriteU8(1);

                using var errorBody = new BinaryBufferWriter();
                errorBody.WriteU8(1);
                errorBody.WriteU32(6002);
                errorBody.WriteString("worker missing");

                notification.WriteU32((uint)errorBody.WrittenMemory.Length);
                notification.WriteBytes(errorBody.WrittenSpan);
                responseHandler?.Invoke(notification.Build());

                return Task.FromResult(Array.Empty<byte>());
            },
            registerNotificationHandler: (_, handler) =>
            {
                responseHandler = handler;
                return new TestRegistration();
            });

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            await foreach (var _ in rpc.CallAsync("rpc://prod/app/echo", new ReadOnlyMemory<byte>("ping"u8.ToArray())))
            {
            }
        });

        // Assert
        Assert.Equal("WORKER_NOT_FOUND", ex.Code);
        Assert.Equal((byte)1, ex.Status);
        Assert.Equal((uint)6002, ex.DomainCode);
        Assert.Contains("worker missing", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(6001u, "TIMEOUT", true)]
    [InlineData(6002u, "WORKER_NOT_FOUND", true)]
    [InlineData(6003u, "BACKPRESSURE", true)]
    [InlineData(6004u, "ROUTE_NOT_REGISTERED", true)]
    [InlineData(6005u, "CORRELATION_NOT_FOUND", false)]
    [InlineData(6006u, "INVALID_SEQUENCE", false)]
    [InlineData(6007u, "DUPLICATE_CORRELATION", false)]
    [InlineData(6008u, "WRONG_WORKER", false)]
    [InlineData(6009u, "UNAUTHORIZED", false)]
    [InlineData(6010u, "BACKEND_ERROR", false)]
    [InlineData(6011u, "INVALID_ROUTE", false)]
    [InlineData(6012u, "INVALID_SUBSCRIPTION_PATTERN", false)]
    [InlineData(6013u, "SUBSCRIPTION_LIMIT", false)]
    public async Task ShouldMapCanonicalErrorGivenRpcDomainCodeWhenCallTerminates(
        uint domainCode,
        string expectedCode,
        bool retryable)
    {
        // Arrange
        Action<byte[]>? responseHandler = null;
        using var rpc = new RpcClient(
            (_, payload, _) =>
            {
                var request = new BinaryBufferReader(payload);
                var correlationId = request.ReadBytes(16);

                using var errorBody = new BinaryBufferWriter();
                errorBody.WriteU8(1);
                errorBody.WriteU32(domainCode);
                errorBody.WriteString(expectedCode);

                using var response = new BinaryBufferWriter();
                response.WriteBytes(correlationId);
                response.WriteU64(0);
                response.WriteU8(1);
                response.WriteU32((uint)errorBody.WrittenMemory.Length);
                response.WriteBytes(errorBody.WrittenSpan);
                responseHandler!(response.Build());
                return Task.FromResult(Array.Empty<byte>());
            },
            registerNotificationHandler: (_, handler) =>
            {
                responseHandler = handler;
                return new TestRegistration();
            });

        // Act
        var error = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            await foreach (var _ in rpc.CallAsync("rpc://prod/app/error", ReadOnlyMemory<byte>.Empty))
            {
            }
        });

        // Assert
        Assert.Equal(expectedCode, error.Code);
        Assert.Equal(domainCode, error.DomainCode);
        Assert.Equal(retryable, Retryability.IsRetryable(error));
    }

    [Fact]
    public async Task ShouldRejectMalformedControlSuccessGivenMissingLengthWhenRegisteringWorker()
    {
        // Arrange
        using var rpc = new RpcClient(
            (_, _, _) => Task.FromResult(new byte[] { 0 }),
            registerNotificationHandler: (_, _) => new TestRegistration());

        // Act
        var act = () => rpc.RegisterWorkerAsync(
            "rpc://prod/app/*",
            (_, _, _) => ValueTask.CompletedTask);

        // Assert
        await Assert.ThrowsAsync<ProtocolException>(act);
    }

    [Fact]
    public async Task ShouldRegisterWorkerAndDispatchRequestGivenIncomingRpcMessageWhenRpcOperationRuns()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;
        Action<byte[]>? incomingHandler = null;
        var requestTcs = new TaskCompletionSource<RpcRequest>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var rpc = new RpcClient(
            (messageType, payload, _) =>
            {
                seenMessageType = messageType;
                seenPayload = payload;

                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteU32(0);
                return Task.FromResult(writer.Build());
            },
            (_, _, _) => Task.CompletedTask,
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.RpcRequest, messageType);
                incomingHandler = handler;
                return new TestRegistration();
            });

        await using var registration = await rpc.RegisterWorkerAsync(
            "rpc://prod/app/echo",
            (req, _, _) =>
            {
                requestTcs.TrySetResult(req);
                return ValueTask.CompletedTask;
            },
            new RpcWorkerOptions { MaxConcurrency = 7 });

        Assert.Equal("rpc://prod/app/echo", registration.Pattern);

        // Act
        Assert.NotNull(incomingHandler);
        using var incoming = new BinaryBufferWriter();
        incoming.WriteBytes(new byte[16]);
        incoming.WriteString("rpc://prod/app/echo");
        incoming.WriteU32(4);
        incoming.WriteBytes("ping"u8);
        incomingHandler!(incoming.Build());

        var request = await requestTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.Equal(MessageTypes.RpcSubscribeWorker, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal("rpc://prod/app/echo", request.Route);
        Assert.Equal("ping", System.Text.Encoding.UTF8.GetString(request.Body.Span));

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("rpc://prod/app/echo", reader.ReadString());
        Assert.Equal((uint)7, reader.ReadU32());
        Assert.True(reader.IsEof);
    }

    [Fact]
    public async Task ShouldReportErrorGivenWorkerHandlerFailureWhenDispatchingRequest()
    {
        // Arrange
        Action<byte[]>? incomingHandler = null;
        var reported = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var rpc = new RpcClient(
            request: (_, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 0, 0, 0, 0, 0 }),
            send: (_, _, _) => ValueTask.CompletedTask,
            registerNotificationHandler: (_, handler) =>
            {
                incomingHandler = handler;
                return new TestRegistration();
            },
            onWorkerError: exception => reported.TrySetResult(exception));
        await using var registration = await rpc.RegisterWorkerAsync(
            "rpc://prod/app/failing",
            (_, _, _) => ValueTask.FromException(new InvalidOperationException("worker failed")));

        using var incoming = new BinaryBufferWriter();
        incoming.WriteBytes(new byte[16]);
        incoming.WriteString("rpc://prod/app/failing");
        incoming.WriteU32(0);
        incomingHandler!(incoming.Build());


        // Act
        var exception = await reported.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.IsType<InvalidOperationException>(exception);
        Assert.Equal("worker failed", exception.Message);
    }

    [Fact]
    public async Task ShouldSerializeResponsesGivenConcurrentSendsWhenWorkerEndsStream()
    {
        // Arrange
        Action<byte[]>? incomingHandler = null;
        var sentPayloads = new List<byte[]>();
        var sentSync = new object();
        var activeSends = 0;
        var maxActiveSends = 0;
        var completed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var rpc = new RpcClient(
            request: (_, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 0, 0, 0, 0, 0 }),
            send: async (_, payload, sendCancellationToken) =>
            {
                var active = Interlocked.Increment(ref activeSends);
                lock (sentSync)
                {
                    maxActiveSends = Math.Max(maxActiveSends, active);
                }
                await Task.Delay(25, sendCancellationToken);
                lock (sentSync)
                {
                    sentPayloads.Add(payload.ToArray());
                }
                Interlocked.Decrement(ref activeSends);
            },
            registerNotificationHandler: (_, handler) =>
            {
                incomingHandler = handler;
                return new TestRegistration();
            });
        await using var registration = await rpc.RegisterWorkerAsync(
            "rpc://prod/app/stream",
            async (_, writer, workerCancellationToken) =>
            {
                await Task.WhenAll(
                    writer.SendAsync("first"u8.ToArray(), ct: workerCancellationToken).AsTask(),
                    writer.SendAsync("last"u8.ToArray(), isEnd: true, workerCancellationToken).AsTask());
                var afterEnd = await Record.ExceptionAsync(() => writer.SendAsync("late"u8.ToArray(), ct: workerCancellationToken).AsTask());
                completed.TrySetResult(afterEnd);
            },
            new RpcWorkerOptions { MaxConcurrency = 2 });

        using var incoming = new BinaryBufferWriter();
        incoming.WriteBytes(new byte[16]);
        incoming.WriteString("rpc://prod/app/stream");
        incoming.WriteU32(0);
        incomingHandler!(incoming.Build());


        // Act
        var afterEndError = await completed.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.IsType<InvalidOperationException>(afterEndError);
        Assert.Equal(1, maxActiveSends);
        Assert.Equal(2, sentPayloads.Count);
        Assert.Equal(new ulong[] { 0, 1 }, sentPayloads.Select(ReadSequence));

        static ulong ReadSequence(byte[] payload)
        {
            var reader = new BinaryBufferReader(payload);
            _ = reader.ReadBytes(16);
            return reader.ReadU64();
        }
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(1025u)]
    public async Task ShouldRejectWorkerConcurrencyOutsideWireRangeGivenRpcClientWhenOperationRuns(uint maxConcurrency)
    {
        // Arrange
        var requestCalled = false;
        using var rpc = new RpcClient(
            (_, _, _) =>
            {
                requestCalled = true;
                return Task.FromResult(Array.Empty<byte>());
            },
            (_, _, _) => Task.CompletedTask,
            (_, _) => new TestRegistration());

        // Act
        var error = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            rpc.RegisterWorkerAsync(
                "rpc://prod/app/echo",
                (_, _, _) => ValueTask.CompletedTask,
                new RpcWorkerOptions { MaxConcurrency = maxConcurrency }));

        // Assert
        Assert.Equal("options", error.ParamName);
        Assert.False(requestCalled);
    }

    [Theory]
    [InlineData(1u)]
    [InlineData(1024u)]
    public async Task ShouldEncodeWorkerConcurrencyAtWireBoundariesGivenRpcClientWhenOperationRuns(uint maxConcurrency)
    {
        // Arrange
        byte[]? seenPayload = null;
        using var rpc = new RpcClient(
            (_, payload, _) =>
            {
                seenPayload = payload.ToArray();
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteU32(0);
                return Task.FromResult(writer.Build());
            },
            (_, _, _) => Task.CompletedTask,
            (_, _) => new TestRegistration());

        // Act
        await using var registration = await rpc.RegisterWorkerAsync(
            "rpc://prod/app/echo",
            (_, _, _) => ValueTask.CompletedTask,
            new RpcWorkerOptions { MaxConcurrency = maxConcurrency });

        // Assert
        Assert.NotNull(seenPayload);
        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("rpc://prod/app/echo", reader.ReadString());
        Assert.Equal(maxConcurrency, reader.ReadU32());
        Assert.True(reader.IsEof);
    }

    [Fact]
    public async Task ShouldTimeOutGivenNoWorkerResponseWhenCallAwaited()
    {
        // Arrange
        using var rpc = new RpcClient(
            (_, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                return Task.FromResult(Array.Empty<byte>());
            },
            registerNotificationHandler: (_, _) => new TestRegistration(),
            connectionTimeout: TimeSpan.FromMilliseconds(50));

        // Act
        var ex = await Assert.ThrowsAsync<RequestTimeoutException>(async () =>
        {
            await foreach (var _ in rpc.CallAsync("rpc://prod/app/slow", "ping"u8.ToArray()))
            {
            }
        });

        // Assert
        Assert.Contains("timed out", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ShouldThrowOperationCanceledGivenCanceledTokenWhenCallingRpc()
    {
        // Arrange
        // Act
        // Assert
        using var rpc = new RpcClient(
            (_, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                return Task.FromResult(Array.Empty<byte>());
            },
            registerNotificationHandler: (_, _) => new TestRegistration(),
            connectionTimeout: TimeSpan.FromSeconds(1));

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        // Act / Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in rpc.CallAsync("rpc://prod/app/cancel", "ping"u8.ToArray(), cts.Token))
            {
            }
        });
    }

    [Fact]
    public async Task ShouldThrowConnectionExceptionGivenConnectionClosedWhenCallingRpc()
    {
        // Arrange
        using var connectionClosed = new CancellationTokenSource();
        using var rpc = new RpcClient(
            (_, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                return Task.FromResult(Array.Empty<byte>());
            },
            registerNotificationHandler: (_, _) => new TestRegistration(),
            getConnectionClosedToken: () => connectionClosed.Token,
            connectionTimeout: TimeSpan.FromSeconds(1));

        var callTask = Task.Run(async () =>
        {
            await foreach (var _ in rpc.CallAsync("rpc://prod/app/disconnect", "ping"u8.ToArray()))
            {
            }
        });

        // Act
        await Task.Delay(50);
        await connectionClosed.CancelAsync();

        var ex = await Assert.ThrowsAsync<ConnectionException>(() => callTask);

        // Assert
        Assert.Equal("Connection closed or reset", ex.Message);
    }

}
