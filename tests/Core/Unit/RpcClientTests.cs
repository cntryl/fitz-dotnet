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

        var rpc = new RpcClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            return Task.FromResult(writer.Build());
        });

        // Act
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        try
        {
            await foreach (var _ in rpc.CallAsync("rpc://prod/app/echo", new ReadOnlyMemory<byte>("ping"u8.ToArray()), cts.Token))
            {
                // no frames expected from a mock that only returns status
            }
        }
        catch (OperationCanceledException) { /* expected when mock sends no response frames */ }

        // Assert
        Assert.Equal(MessageTypes.RpcRequest, seenMessageType);
        Assert.NotNull(seenPayload);

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
        var rpc = new RpcClient((_, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(7);
            return Task.FromResult(writer.Build());
        });

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
                return Task.FromResult(writer.Build());
            },
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.RpcRequest, messageType);
                incomingHandler = handler;
            },
            _ => { });

        await rpc.RegisterWorkerAsync(
            "rpc://prod/app/*",
            req =>
            {
                requestTcs.TrySetResult(req);
                return Task.FromResult<IRpcResponseWriter>(new NoopWriter());
            });

        // Act
        Assert.NotNull(incomingHandler);
        using var incoming = new BinaryBufferWriter();
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
        Assert.Equal("rpc://prod/app/*", reader.ReadString());
    }

    private sealed class NoopWriter : IRpcResponseWriter
    {
        public Task SendAsync(ReadOnlyMemory<byte> body, bool isEnd = false, CancellationToken ct = default)
        {
            return Task.CompletedTask;
        }
    }
}