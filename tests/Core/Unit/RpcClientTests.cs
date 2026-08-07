using System.Runtime.CompilerServices;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RpcClientTests
{
    [Fact]
    public async Task should_register_request_handler_once_given_concurrent_worker_registration()
    {
        var bothRequestsStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseResponses = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var requestCount = 0;
        var handlerRegistrations = 0;
        var rpc = new RpcClient(
            async (_, _, _) =>
            {
                if (Interlocked.Increment(ref requestCount) == 2)
                {
                    bothRequestsStarted.TrySetResult();
                }
                await releaseResponses.Task;
                return new byte[] { 0 };
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
    public async Task should_send_rpc_request_and_yield_response_frames_given_valid_stream_when_invoking_rpc()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;
        Action<byte[]>? responseHandler = null;

        var rpc = new RpcClient(
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
    public async Task should_throw_rpc_exception_given_terminal_error_response_when_calling_rpc()
    {
        Action<byte[]>? responseHandler = null;
        var rpc = new RpcClient(
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

    [Fact]
    public async Task should_register_worker_and_dispatch_request_given_incoming_rpc_message()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;
        Action<byte[]>? incomingHandler = null;
        var requestTcs = new TaskCompletionSource<RpcRequest>(TaskCreationOptions.RunContinuationsAsynchronously);

        var rpc = new RpcClient(
            (messageType, payload, _) =>
            {
                seenMessageType = messageType;
                seenPayload = payload;

                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
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
    public async Task should_time_out_given_no_worker_response_when_call_awaited()
    {
        // Arrange
        var rpc = new RpcClient(
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
    public async Task should_throw_operation_canceled_given_canceled_token_when_calling_rpc()
    {
        // Arrange
        var rpc = new RpcClient(
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
    public async Task should_throw_connection_exception_given_connection_closed_when_calling_rpc()
    {
        // Arrange
        using var connectionClosed = new CancellationTokenSource();
        var rpc = new RpcClient(
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
