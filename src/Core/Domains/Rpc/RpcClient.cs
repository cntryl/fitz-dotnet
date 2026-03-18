using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Rpc;

public sealed class RpcClient : IRpcClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;
    private readonly Action<ushort, Action<byte[]>>? _registerNotificationHandler;
    private readonly Action<ushort>? _unregisterNotificationHandler;

    internal RpcClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterNotificationHandler,
            connection.UnregisterNotificationHandler)
    {
    }

    public RpcClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Action<ushort, Action<byte[]>>? registerNotificationHandler = null,
        Action<ushort>? unregisterNotificationHandler = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
        _unregisterNotificationHandler = unregisterNotificationHandler;
    }

    public async IAsyncEnumerable<RpcResponseFrame> CallAsync(
        string route,
        ReadOnlyMemory<byte> body,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var correlationId = new byte[16];
        RandomNumberGenerator.Fill(correlationId);

        // Register to receive response frames
        var channel = new SubscriptionChannel<RpcResponseFrame>();
        _registerNotificationHandler?.Invoke(MessageTypes.RpcResponse, payload =>
        {
            try
            {
                var reader = new BinaryBufferReader(payload);
                var corrId = reader.ReadBytes(16);
                if (AreByteArraysEqual(corrId, correlationId))
                {
                    var sequence = reader.ReadU64();
                    var bodyLength = reader.ReadU32();
                    var responseBody = reader.ReadBytes((int)bodyLength);
                    channel.PostNotification(new RpcResponseFrame(responseBody.AsMemory(), sequence));
                }
            }
            catch
            {
                channel.Dispose();
            }
        });

        using var writer = new BinaryBufferWriter();
        writer.WriteU32((uint)correlationId.Length);
        writer.WriteBytes(correlationId);
        writer.WriteString(route);
        writer.WriteString(string.Empty);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body.Span);

        try
        {
            var response = await _request(MessageTypes.RpcRequest, writer.Build(), ct);
            var reader = new BinaryBufferReader(response);
            var status = reader.ReadU8();
            if (status != 0)
            {
                throw new RpcException($"CALL failed with status {status}", "CALL_FAILED", status);
            }
        }
        catch
        {
            _unregisterNotificationHandler?.Invoke(MessageTypes.RpcResponse);
            throw;
        }

        // Yield response frames from the channel
        await foreach (var frame in channel.GetEnumerableAsync(ct))
        {
            yield return frame;
        }
    }

    public async Task RegisterWorkerAsync(
        string pattern,
        Func<RpcRequest, Task<IRpcResponseWriter>> handler,
        CancellationToken ct = default)
    {
        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for worker registration");
        }

        // Register to receive incoming RPC requests
        _registerNotificationHandler(MessageTypes.RpcRequest, payload =>
        {
            _ = HandleIncomingRequestAsync(payload, handler, ct);
        });

        // Send worker registration request
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.RpcSubscribeWorker, writer.Build(), ct);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new RpcException($"REGISTER failed with status {status}", "REGISTER_FAILED", status);
        }
    }

    private async Task HandleIncomingRequestAsync(
        byte[] payload,
        Func<RpcRequest, Task<IRpcResponseWriter>> handler,
        CancellationToken ct)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            var route = reader.ReadString();
            var bodyLength = reader.ReadU32();
            var body = reader.ReadBytes((int)bodyLength);

            var request = new RpcRequest(route, body.AsMemory());
            var writer = new RpcResponseImpl(_request);
            await handler(request);
            // Writer will have sent response frames
        }
        catch (Exception ex)
        {
            // Log error but don't throw; this is a background handler
            System.Diagnostics.Debug.WriteLine($"RPC handler error: {ex}");
        }
    }

    private static bool AreByteArraysEqual(byte[] a, byte[] b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
        {
            if (a[i] != b[i]) return false;
        }
        return true;
    }

    private sealed class RpcResponseImpl : IRpcResponseWriter
    {
        private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;

        public RpcResponseImpl(Func<ushort, byte[], CancellationToken, Task<byte[]>> request)
        {
            _request = request;
        }

        public async Task SendAsync(ReadOnlyMemory<byte> body, bool isEnd = false, CancellationToken ct = default)
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(isEnd ? (byte)1 : (byte)0);
            writer.WriteU32((uint)body.Length);
            writer.WriteBytes(body.Span);

            await _request(MessageTypes.RpcResponse, writer.Build(), ct);
        }
    }
}