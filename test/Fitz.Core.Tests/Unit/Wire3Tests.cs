using System.Buffers.Binary;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Domains.Queue;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class Wire3Tests
{
    public static TheoryData<string, string> Vectors
    {
        get
        {
            var result = new TheoryData<string, string>();
            foreach (var line in File.ReadLines(Path.Combine(AppContext.BaseDirectory, "Fixtures", "wire3_vectors.txt")))
            {
                var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                result.Add(parts[0], parts[1]);
            }
            return result;
        }
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public void ShouldDecodeSharedVectorGivenWire3Frame(string name, string hex)
    {
        // Arrange
        var frame = FrameCodec.DecodeStrict(Convert.FromHexString(hex));
        // Act
        var decoded = ExtensionCodec.Decode(frame);
        // Assert
        Assert.False(decoded.Core.IsEmpty);
        if (name == "hello_wire3_queue_capabilities")
            Assert.Equal(6u, decoded.CapabilityMask);
        if (name == "unknown_extension_type104_corr1_core_hi")
            Assert.Equal("hi"u8.ToArray(), decoded.Core.ToArray());
    }

    [Theory]
    [InlineData("")]
    [InlineData("00000000")]
    [InlineData("00000009")]
    [InlineData("0000000001")]
    [InlineData("00000000000000000000")]
    [InlineData("000000000009ffffffff")]
    public void ShouldRejectMalformedEnvelopeGivenExtensionFlag(string hex)
    {
        // Arrange
        var frame = new Frame(104, 4, 1, Convert.FromHexString(hex));
        // Act
        Action decode = () => { _ = ExtensionCodec.Decode(frame); };
        // Assert
        Assert.Throws<ProtocolException>(decode);
    }

    [Fact]
    public void ShouldRejectDuplicateCapabilitiesGivenHelloSingleton()
    {
        // Arrange
        var frame = new Frame(2, 4, 0, Convert.FromHexString("0000000000010000000200000001000000020000"));
        // Act
        Action decode = () => { _ = ExtensionCodec.Decode(frame); };
        // Assert
        Assert.Throws<ProtocolException>(decode);
    }

    [Fact]
    public async Task ShouldEncodeTtlGivenNegotiatedQueueCapability()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(new ClientConfig(new Uri("ws://queued/ws")), () => transport);
        await connection.ConnectAsync();
        using var queue = new QueueClient(connection);
        transport.QueueResponse(MessageTypes.QueueEnqueue, [0, 0, 0, 0, 0, 0, 0, 0, 1]);
        // Act
        var id = await queue.EnqueueWithOptionsAsync("queue://acme/jobs/q", "job"u8.ToArray(), new QueueEnqueueOptions(TtlHours: 2));
        // Assert
        Assert.Equal(1UL, id);
        var frame = FrameCodec.DecodeStrict(transport.SentFrames[1]);
        Assert.Equal(ExtensionCodec.PresentFlag, frame.Flags);
        Assert.Equal(2u, BinaryPrimitives.ReadUInt32BigEndian(frame.Payload.Span[^4..]));
        Assert.Equal("queue://acme/jobs/q", new BinaryBufferReader(ExtensionCodec.Decode(frame).Core).ReadString());
    }

    [Fact]
    public async Task ShouldCloseReservationGivenSuccessfulNack()
    {
        // Arrange
        ushort sentType = 0;
        ReadOnlyMemory<byte> sentPayload = default;
        var item = new QueueReservedItem("queue://acme/jobs/q", default, 0, 1, 9, (type, payload, _) =>
        {
            sentType = type;
            sentPayload = payload.ToArray();
            return ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 0 });
        }, supportsNack: _ => true);
        await using var reservation = item;
        // Act
        await item.NackAsync(TimeSpan.FromSeconds(60));
        // Assert
        Assert.Equal(MessageTypes.QueueNack, sentType);
        Assert.Equal(60UL, BinaryPrimitives.ReadUInt64BigEndian(sentPayload.Span[^8..]));
        await Assert.ThrowsAsync<QueueException>(() => item.CompleteAsync());
    }

    [Fact]
    public async Task ShouldRejectNackGivenCapabilityNotNegotiated()
    {
        // Arrange
        var sent = false;
        await using var item = new QueueReservedItem("queue://acme/jobs/q", default, 0, 1, 9, (_, _, _) =>
        {
            sent = true;
            return ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 0 });
        });
        // Act
        var error = await Record.ExceptionAsync(() => item.NackAsync());
        // Assert
        Assert.IsType<NotSupportedException>(error);
        Assert.False(sent);
    }

    [Fact]
    public async Task ShouldRejectOversizedRequestGivenWire3NegotiatedLimit()
    {
        // Arrange
        await using var transport = new TestQueuedTransport { AutoHello = false };
        var hello = ExtensionCodec.Decode(FrameCodec.DecodeStrict(ConnectionNegotiationTests.HelloFrame())).Core.ToArray();
        BinaryPrimitives.WriteUInt32BigEndian(hello.AsSpan(6), 128);
        transport.QueueIncomingFrame(FrameCodec.Encode(2, ExtensionCodec.Encode(hello, 1, ExtensionCodec.ClientCapabilities), 4));
        await using var connection = new FitzConnection(new ClientConfig(new Uri("ws://queued/ws"), Timeout: TimeSpan.FromMilliseconds(100)), () => transport);
        await connection.ConnectAsync();
        // Act
        var error = await Record.ExceptionAsync(() => connection.RequestAsync(MessageTypes.KvPut, new byte[128]).AsTask());
        // Assert
        Assert.IsType<ProtocolException>(error);
        Assert.Single(transport.SentFrames);
    }

    [Fact]
    public async Task ShouldExposeCapabilityIdsGivenNegotiatedSession()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var client = new Client(new ClientConfig(new Uri("ws://queued/ws"), TransportFactory: _ => transport));
        // Act
        await client.ConnectAsync();
        // Assert
        Assert.Equal(new uint[] { 1, 2 }, ((IClient)client).CapabilityIds);
    }

    [Fact]
    public async Task ShouldClearCapabilityIdsGivenSessionWhenClosed()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var client = new Client(new ClientConfig(new Uri("ws://queued/ws"), TransportFactory: _ => transport));
        await client.ConnectAsync();
        // Act
        await client.CloseAsync();
        // Assert
        Assert.Empty(((IClient)client).CapabilityIds);
    }
}
