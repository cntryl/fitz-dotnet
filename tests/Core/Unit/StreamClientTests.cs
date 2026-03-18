using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class StreamClientTests
{
    [Fact]
    public async Task BeginAsync_EncodesRequestAndReturnsSession()
    {
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var stream = new StreamClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteU64(99);
            return Task.FromResult(writer.Build());
        });

        var session = await stream.BeginAsync("stream://prod/app/events", 12, "meta"u8.ToArray());

        Assert.NotNull(session);
        Assert.Equal(MessageTypes.StreamBegin, seenMessageType);
        Assert.NotNull(seenPayload);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("stream://prod/app/events", reader.ReadString());
        Assert.Equal((ulong)12, reader.ReadU64());
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((uint)4, reader.ReadU32());
        Assert.Equal("meta", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(4)));
    }

    [Fact]
    public async Task ReadAsync_DecodesRecords()
    {
        var stream = new StreamClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.StreamRead, messageType);

            var request = new BinaryBufferReader(payload);
            Assert.Equal("stream://prod/app/events", request.ReadString());
            Assert.Equal((ulong)4, request.ReadU64());
            Assert.Equal((ulong)2, request.ReadU64());
            Assert.Equal((byte)0, request.ReadU8());

            var data = new BinaryBufferWriter();
            data.WriteU32(2);
            data.WriteU64(4);
            data.WriteU32(3);
            data.WriteBytes("one"u8);
            data.WriteU64(5);
            data.WriteU32(3);
            data.WriteBytes("two"u8);

            var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(0);
            writer.WriteU32((uint)data.Build().Length);
            writer.WriteBytes(data.Build());
            return Task.FromResult(writer.Build());
        });

        var records = await stream.ReadAsync("stream://prod/app/events", 4, 2);

        Assert.Collection(
            records,
            record =>
            {
                Assert.Equal((ulong)4, record.Offset);
                Assert.Equal("one", System.Text.Encoding.UTF8.GetString(record.Body));
            },
            record =>
            {
                Assert.Equal((ulong)5, record.Offset);
                Assert.Equal("two", System.Text.Encoding.UTF8.GetString(record.Body));
            });
    }

    [Fact]
    public async Task MetadataAsync_DecodesMetadata()
    {
        var stream = new StreamClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.StreamGetMetadata, messageType);

            var request = new BinaryBufferReader(payload);
            Assert.Equal("stream://prod/app/events", request.ReadString());

            var data = new BinaryBufferWriter();
            data.WriteU64(10);
            data.WriteU64(42);
            data.WriteU64(33);

            var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(0);
            writer.WriteU32((uint)data.Build().Length);
            writer.WriteBytes(data.Build());
            return Task.FromResult(writer.Build());
        });

        var metadata = await stream.MetadataAsync("stream://prod/app/events");

        Assert.Equal(new StreamMetadata(10, 42, 33), metadata);
    }
}