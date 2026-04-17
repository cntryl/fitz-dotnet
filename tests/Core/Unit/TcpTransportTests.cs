using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class TcpTransportTests
{
    [Fact]
    public async Task should_send_and_receive_length_prefixed_frames_given_tcp_transport()
    {
        // Arrange
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();

        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var serverTask = Task.Run(async () =>
        {
            using var server = await listener.AcceptTcpClientAsync();
            await using var stream = server.GetStream();

            var request = await ReadFrameAsync(stream);
            Assert.Equal("ping", System.Text.Encoding.UTF8.GetString(request));

            await WriteFrameAsync(stream, "pong"u8.ToArray());
        });

        await using var transport = new TcpTransport($"127.0.0.1:{port}", TimeSpan.FromSeconds(2), 64 * 1024);

        // Act
        await transport.ConnectAsync();
        await transport.SendAsync("ping"u8.ToArray());
        using var response = await transport.ReceiveAsync();

        // Assert
        Assert.Equal("pong", System.Text.Encoding.UTF8.GetString(response.Memory.Span));

        await serverTask;
        listener.Stop();
    }

    private static async Task<byte[]> ReadFrameAsync(NetworkStream stream)
    {
        var header = new byte[4];
        await ReadExactAsync(stream, header);
        var length = checked((int)BinaryPrimitives.ReadUInt32BigEndian(header));
        var payload = new byte[length];
        await ReadExactAsync(stream, payload);
        return payload;
    }

    private static async Task WriteFrameAsync(NetworkStream stream, byte[] payload)
    {
        var header = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(header, (uint)payload.Length);
        await stream.WriteAsync(header);
        await stream.WriteAsync(payload);
        await stream.FlushAsync();
    }

    private static async Task ReadExactAsync(NetworkStream stream, byte[] buffer)
    {
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead));
            if (read == 0)
            {
                throw new EndOfStreamException("Unexpected EOF");
            }

            totalRead += read;
        }
    }
}
