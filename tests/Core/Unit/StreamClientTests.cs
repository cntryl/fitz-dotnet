using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class StreamClientTests
{
    [Fact]
    public async Task should_return_stream_session_given_success_response_when_beginning_stream()
    {
        // Arrange
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

        // Act
        var session = await stream.BeginAsync("stream://prod/app/events", 12, "meta"u8.ToArray());

        // Assert
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
    public async Task should_return_records_given_wrapped_payload_when_reading_stream()
    {
        // Arrange
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

        // Act
        var records = new List<StreamRecord>();
        await foreach (var record in stream.ReadAsync("stream://prod/app/events", 4, 2))
        {
            records.Add(record);
        }

        // Assert
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
    public async Task should_return_metadata_given_wrapped_payload_when_reading_stream_metadata()
    {
        // Arrange
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

        // Act
        var metadata = await stream.MetadataAsync("stream://prod/app/events");

        // Assert
        Assert.Equal(new StreamMetadata(10, 42, 33), metadata);
    }

    [Fact]
    public async Task should_return_committed_offset_given_append_response_when_appending_to_stream_session()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();

        var stream = new StreamClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            var writer = new BinaryBufferWriter();
            if (messageType == MessageTypes.StreamBegin)
            {
                writer.WriteU8(0);
                writer.WriteU8(1);
                writer.WriteU64(99);
            }
            else
            {
                writer.WriteU8(0);
                writer.WriteU8(0);
                writer.WriteU32(8);
                writer.WriteU64(1234);
            }

            return Task.FromResult(writer.Build());
        });

        var session = await stream.BeginAsync("stream://prod/app/events");

        // Act
        var offset = await session.AppendAsync("entry"u8.ToArray(), "meta"u8.ToArray());

        // Assert
        Assert.Equal((ulong)1234, offset);
        Assert.Equal(2, calls.Count);
        Assert.Equal(MessageTypes.StreamAppend, calls[1].MessageType);

        var reader = new BinaryBufferReader(calls[1].Payload);
        Assert.Equal((ulong)99, reader.ReadU64());
        Assert.Equal((uint)5, reader.ReadU32());
        Assert.Equal("entry", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(5)));
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((uint)4, reader.ReadU32());
        Assert.Equal("meta", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(4)));
    }

    [Fact]
    public async Task should_encode_commit_flag_given_stream_session_when_committing()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();

        var stream = new StreamClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (messageType == MessageTypes.StreamBegin)
            {
                writer.WriteU8(1);
                writer.WriteU64(44);
            }

            return Task.FromResult(writer.Build());
        });

        var session = await stream.BeginAsync("stream://prod/app/events");

        // Act
        await session.CommitAsync();

        // Assert
        Assert.Equal(2, calls.Count);
        Assert.Equal(MessageTypes.StreamBegin, calls[0].MessageType);
        Assert.Equal(MessageTypes.StreamCommit, calls[1].MessageType);

        var commitReader = new BinaryBufferReader(calls[1].Payload);
        Assert.Equal((ulong)44, commitReader.ReadU64());
        Assert.Equal((byte)0, commitReader.ReadU8());
    }

    [Fact]
    public async Task should_encode_session_id_given_stream_session_when_rolling_back()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();

        var stream = new StreamClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (messageType == MessageTypes.StreamBegin)
            {
                writer.WriteU8(1);
                writer.WriteU64(44);
            }

            return Task.FromResult(writer.Build());
        });

        var session = await stream.BeginAsync("stream://prod/app/events");

        // Act
        await session.RollbackAsync();

        // Assert
        Assert.Equal(2, calls.Count);
        Assert.Equal(MessageTypes.StreamBegin, calls[0].MessageType);
        Assert.Equal(MessageTypes.StreamRollback, calls[1].MessageType);

        var rollbackReader = new BinaryBufferReader(calls[1].Payload);
        Assert.Equal((ulong)44, rollbackReader.ReadU64());
    }
}