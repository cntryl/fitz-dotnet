using System.Runtime.CompilerServices;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RpcClientTests
{
    [Fact]
    public async Task should_accept_success_status_given_valid_request_when_invoking_rpc()
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

                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteString("ready");
                return Task.FromResult(writer.Build());
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
        notification.WriteU32(16);
        var requestReader = new BinaryBufferReader(seenPayload!);
        _ = requestReader.ReadU32();
        var correlationId = requestReader.ReadBytes(16);
        notification.WriteBytes(correlationId);
        notification.WriteU64(0);
        notification.WriteU32(4);
        notification.WriteBytes("pong"u8);
        notification.WriteU8(1);
        responseHandler!(notification.Build());

        await task;

        // Assert
        Assert.Equal(MessageTypes.RpcRequest, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Single(frames);
        Assert.Equal("pong", System.Text.Encoding.UTF8.GetString(frames[0].Body.Span));

        var reader = new BinaryBufferReader(seenPayload!);
        var corrLen = reader.ReadU32();
        Assert.Equal((uint)16, corrLen);
        _ = reader.ReadBytes((int)corrLen);
        Assert.Equal("rpc://prod/app/echo", reader.ReadString());
        Assert.Equal(string.Empty, reader.ReadString());
        Assert.Equal((uint)4, reader.ReadU32());
        Assert.Equal("ping", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(4)));
    }

    [Fact]
    public async Task should_throw_rpc_exception_given_non_zero_status_when_calling_rpc()
    {
        // Arrange
        var rpc = new RpcClient(
            (_, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(7);
                return Task.FromResult(writer.Build());
            },
            registerNotificationHandler: (_, _) => new TestRegistration());

        // Act
        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            await foreach (var _ in rpc.CallAsync("rpc://prod/app/echo", new ReadOnlyMemory<byte>("ping"u8.ToArray())))
            {
            }
        });

        // Assert
        Assert.Equal("CALL_FAILED", ex.Code);
        Assert.Equal((byte)7, ex.Status);
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
                writer.WriteU64(42);
                return Task.FromResult(writer.Build());
            },
            (_, _, _) => Task.CompletedTask,
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.RpcRequest, messageType);
                incomingHandler = handler;
                return new TestRegistration();
            });

        using var registration = await rpc.RegisterWorkerAsync(
            "rpc://prod/app/echo",
            (req, _, _) =>
            {
                requestTcs.TrySetResult(req);
                return Task.CompletedTask;
            });

        // Act
        Assert.NotNull(incomingHandler);
        using var incoming = new BinaryBufferWriter();
        incoming.WriteU32(16);
        incoming.WriteBytes(new byte[16]);
        incoming.WriteString("rpc://prod/app/echo");
        incoming.WriteString(string.Empty);
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
    }

    [Fact]
    public async Task should_throw_request_timeout_given_no_response_frames_when_calling_rpc()
    {
        // Arrange
        var rpc = new RpcClient(
            (_, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                return Task.FromResult(writer.Build());
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
                writer.WriteU8(0);
                return Task.FromResult(writer.Build());
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
                writer.WriteU8(0);
                return Task.FromResult(writer.Build());
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
        connectionClosed.Cancel();

        var ex = await Assert.ThrowsAsync<ConnectionException>(() => callTask);

        // Assert
        Assert.Equal("Connection closed or reset", ex.Message);
    }

    [Fact]
    public async Task should_forward_wildcard_route_without_local_validation_when_calling_rpc()
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
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                return Task.FromResult(writer.Build());
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
            await foreach (var frame in rpc.CallAsync("rpc://prod/app/*", "ping"u8.ToArray(), cts.Token))
            {
                frames.Add(frame);
            }
        });

        await Task.Delay(25);
        Assert.NotNull(responseHandler);
        using var notification = new BinaryBufferWriter();
        notification.WriteU32(16);
        var requestReader = new BinaryBufferReader(seenPayload!);
        _ = requestReader.ReadU32();
        var correlationId = requestReader.ReadBytes(16);
        notification.WriteBytes(correlationId);
        notification.WriteU64(0);
        notification.WriteU32(4);
        notification.WriteBytes("pong"u8);
        notification.WriteU8(1);
        responseHandler!(notification.Build());

        await task;

        // Assert
        Assert.Single(frames);
        Assert.Equal(MessageTypes.RpcRequest, seenMessageType);
        var reader = new BinaryBufferReader(seenPayload!);
        _ = reader.ReadU32();
        _ = reader.ReadBytes(16);
        Assert.Equal("rpc://prod/app/*", reader.ReadString());
    }
}
