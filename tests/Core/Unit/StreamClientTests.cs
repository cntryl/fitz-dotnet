using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class StreamClientTests
{
    public static TheoryData<string, string> ExactRouteValidationCases => new()
    {
        { "queue://prod/app/events", "must start with stream://" },
        { "stream://prod//events", "segments must be non-empty" },
        { "stream://prod/app/*", "must be stream://{realm}/{area}/{resource}" },
        { "stream://prod/*/*", "must be stream://{realm}/{area}/{resource}" },
        { "stream://prod/app/events/extra", "must be stream://{realm}/{area}/{resource}" },
    };

    public static TheoryData<string, string> SelectorRouteValidationCases => new()
    {
        { "queue://prod/app/events", "must start with stream://" },
        { "stream://prod//events", "segments must be non-empty" },
        { "stream://prod/*", "must be one of stream://{realm}/{area}/{resource}, stream://{realm}/{area}/*, or stream://{realm}/*/*" },
        { "stream://prod/*/events", "must be one of stream://{realm}/{area}/{resource}, stream://{realm}/{area}/*, or stream://{realm}/*/*" },
        { "stream://*/app/events", "must be one of stream://{realm}/{area}/{resource}, stream://{realm}/{area}/*, or stream://{realm}/*/*" },
        { "stream://prod/app/**", "must be one of stream://{realm}/{area}/{resource}, stream://{realm}/{area}/*, or stream://{realm}/*/*" },
        { "stream://prod/app/events/extra", "must be one of stream://{realm}/{area}/{resource}, stream://{realm}/{area}/*, or stream://{realm}/*/*" },
    };

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

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteU64(99);
            return Task.FromResult(writer.Build());
        });

        // Act
        var session = await stream.BeginAsync("stream://prod/app/events", "meta"u8.ToArray());

        // Assert
        Assert.NotNull(session);
        Assert.Equal(MessageTypes.StreamBegin, seenMessageType);
        Assert.NotNull(seenPayload);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("stream://prod/app/events", reader.ReadString());
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((uint)4, reader.ReadU32());
        Assert.Equal("meta", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(4)));
    }

    [Fact]
    public async Task should_allow_optional_payload_given_begin_response_when_beginning_stream()
    {
        // Arrange
        var stream = new StreamClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.StreamBegin, messageType);

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteU64(99);
            writer.WriteU32(0);
            return Task.FromResult(writer.Build());
        });

        // Act
        var session = await stream.BeginAsync("stream://prod/app/events");

        // Assert
        Assert.NotNull(session);
    }

    [Theory]
    [MemberData(nameof(ExactRouteValidationCases))]
    public async Task should_reject_invalid_route_given_exact_stream_methods_when_calling_begin_peek_and_metadata(string route, string expectedMessage)
    {
        // Arrange
        var requestCount = 0;
        var stream = new StreamClient((messageType, payload, _) =>
        {
            requestCount++;
            return Task.FromResult(Array.Empty<byte>());
        });

        // Act
        var beginEx = await Assert.ThrowsAsync<StreamException>(async () =>
        {
            await stream.BeginAsync(route);
        });

        var peekEx = await Assert.ThrowsAsync<StreamException>(async () =>
        {
            await stream.PeekAsync(route);
        });

        var metadataEx = await Assert.ThrowsAsync<StreamException>(async () =>
        {
            await stream.MetadataAsync(route);
        });

        // Assert
        Assert.Equal("INVALID_ROUTE", beginEx.Code);
        Assert.Equal("INVALID_ROUTE", peekEx.Code);
        Assert.Equal("INVALID_ROUTE", metadataEx.Code);
        Assert.Contains(expectedMessage, beginEx.Message, StringComparison.Ordinal);
        Assert.Contains(expectedMessage, peekEx.Message, StringComparison.Ordinal);
        Assert.Contains(expectedMessage, metadataEx.Message, StringComparison.Ordinal);
        Assert.Equal(0, requestCount);
    }

    [Theory]
    [MemberData(nameof(SelectorRouteValidationCases))]
    public async Task should_reject_invalid_route_given_stream_read_and_subscribe_when_using_selector_methods(string route, string expectedMessage)
    {
        // Arrange
        var requestCount = 0;
        var stream = new StreamClient(
            (messageType, payload, _) =>
            {
                requestCount++;
                return Task.FromResult(Array.Empty<byte>());
            },
            (_, _) => new TestRegistration());

        // Act
        var readEx = await Assert.ThrowsAsync<StreamException>(async () =>
        {
            await foreach (var _ in stream.ReadAsync(route, 0, 1))
            {
            }
        });

        var subscribeEx = await Assert.ThrowsAsync<StreamException>(async () =>
        {
            await stream.SubscribeAsync(route, (evt, cancellationToken) => ValueTask.CompletedTask);
        });

        // Assert
        Assert.Equal("INVALID_ROUTE", readEx.Code);
        Assert.Equal("INVALID_ROUTE", subscribeEx.Code);
        Assert.Contains(expectedMessage, readEx.Message, StringComparison.Ordinal);
        Assert.Contains(expectedMessage, subscribeEx.Message, StringComparison.Ordinal);
        Assert.Equal(0, requestCount);
    }

    [Fact]
    public async Task should_accept_wildcard_selector_given_stream_read_when_reading_stream()
    {
        // Arrange
        var requestCount = 0;
        var stream = new StreamClient((messageType, payload, _) =>
        {
            requestCount++;
            Assert.Equal(MessageTypes.StreamRead, messageType);

            var request = new BinaryBufferReader(payload);
            Assert.Equal("stream://prod/app/*", request.ReadString());
            Assert.Equal((ulong)4, request.ReadU64());
            Assert.Equal((ulong)2, request.ReadU64());
            Assert.Equal((byte)0, request.ReadU8());

            return Task.FromResult(new byte[] { 0 });
        });

        // Act
        var records = new List<StreamRecord>();
        await foreach (var record in stream.ReadAsync("stream://prod/app/*", 4, 2))
        {
            records.Add(record);
        }

        // Assert
        Assert.Empty(records);
        Assert.Equal(1, requestCount);
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

            using var data = new BinaryBufferWriter();
            data.WriteU32(2);
            data.WriteU64(4);
            data.WriteU8(0);
            data.WriteU8(0);
            data.WriteU32(3);
            data.WriteBytes("one"u8);
            data.WriteU8(0);
            data.WriteU64(111);
            data.WriteU64(5);
            data.WriteU8(0);
            data.WriteU8(0);
            data.WriteU32(3);
            data.WriteBytes("two"u8);
            data.WriteU8(0);
            data.WriteU64(222);
            data.WriteU64(5);
            data.WriteU8(0);
            data.WriteU8(0);
            data.WriteU8(0);

            using var writer = new BinaryBufferWriter();
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
    public async Task should_reject_flat_payload_given_wrapped_payload_when_reading_stream()
    {
        // Arrange
        var requestCount = 0;
        var stream = new StreamClient((messageType, payload, _) =>
        {
            requestCount++;
            Assert.Equal(MessageTypes.StreamRead, messageType);

            var request = new BinaryBufferReader(payload);
            Assert.Equal("stream://prod/app/events", request.ReadString());
            Assert.Equal((ulong)4, request.ReadU64());
            Assert.Equal((ulong)2, request.ReadU64());
            Assert.Equal((byte)0, request.ReadU8());

            using var flat = new BinaryBufferWriter();
            flat.WriteU64(4);
            flat.WriteU8(0);
            flat.WriteU8(0);
            flat.WriteU32(3);
            flat.WriteBytes("one"u8);

            var flatPayload = flat.Build();
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(0);
            writer.WriteU32((uint)flatPayload.Length);
            writer.WriteBytes(flatPayload);
            return Task.FromResult(writer.Build());
        });

        // Act
        var act = async () =>
        {
            await foreach (var _ in stream.ReadAsync("stream://prod/app/events", 4, 2))
            {
            }
        };

        // Assert
        var ex = await Assert.ThrowsAsync<StreamException>(act);
        Assert.Equal("READ_INVALID_RESPONSE", ex.Code);
        Assert.Equal(1, requestCount);
    }

    [Fact]
    public async Task should_reject_trailing_bytes_given_count_prefixed_payload_when_reading_stream()
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

            using var data = new BinaryBufferWriter();
            data.WriteU32(1);
            data.WriteU64(4);
            data.WriteU8(0);
            data.WriteU8(0);
            data.WriteU32(3);
            data.WriteBytes("one"u8);
            data.WriteU8(0);
            data.WriteU64(111);
            data.WriteU64(4);
            data.WriteU8(0);
            data.WriteU8(0);
            data.WriteU8(0);
            data.WriteU8(0xFF);

            var dataPayload = data.Build();
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(0);
            writer.WriteU32((uint)dataPayload.Length);
            writer.WriteBytes(dataPayload);
            return Task.FromResult(writer.Build());
        });

        // Act
        var act = async () =>
        {
            await foreach (var _ in stream.ReadAsync("stream://prod/app/events", 4, 2))
            {
            }
        };

        // Assert
        var ex = await Assert.ThrowsAsync<StreamException>(act);
        Assert.Equal("READ_INVALID_RESPONSE", ex.Code);
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

            using var data = new BinaryBufferWriter();
            data.WriteU8(1);
            data.WriteU64(10);
            data.WriteU8(1);
            data.WriteU64(42);
            data.WriteU64(33);
            data.WriteU64(500);
            data.WriteU64(1024);
            data.WriteU8(0);
            data.WriteU64(100);
            data.WriteU64(200);

            using var writer = new BinaryBufferWriter();
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
    public async Task should_return_last_record_given_wrapped_payload_when_peeking_stream()
    {
        // Arrange
        var stream = new StreamClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.StreamLast, messageType);

            var request = new BinaryBufferReader(payload);
            Assert.Equal("stream://prod/app/events", request.ReadString());

            using var data = new BinaryBufferWriter();
            data.WriteU64(42);
            data.WriteU8(0);
            data.WriteU8(0);
            data.WriteU32(4);
            data.WriteBytes("tail"u8);
            data.WriteU8(0);
            data.WriteU64(123);

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(0);
            writer.WriteU32((uint)data.Build().Length);
            writer.WriteBytes(data.Build());
            return Task.FromResult(writer.Build());
        });

        // Act
        var record = await stream.PeekAsync("stream://prod/app/events");

        // Assert
        Assert.NotNull(record);
        Assert.Equal((ulong)42, record!.Offset);
        Assert.Equal("tail", System.Text.Encoding.UTF8.GetString(record.Body));
    }

    [Fact]
    public async Task should_invoke_stream_handler_given_matching_notification_when_subscribing()
    {
        // Arrange
        Action<byte[]>? notifyHandler = null;
        StreamCommitEvent? received = null;
        CancellationToken seenCancellationToken = default;
        var receivedTcs = new TaskCompletionSource<StreamCommitEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stream = new StreamClient(
            (messageType, payload, _) =>
            {
                var request = new BinaryBufferReader(payload);
                using var writer = new BinaryBufferWriter();

                if (messageType == MessageTypes.StreamSubscribe)
                {
                    Assert.Equal("stream://prod/*/*", request.ReadString());
                    writer.WriteU8(0);
                    writer.WriteU8(1);
                    writer.WriteU64(55);
                }
                else
                {
                    Assert.Equal(MessageTypes.StreamUnsubscribe, messageType);
                    Assert.Equal("stream://prod/*/*", request.ReadString());
                    writer.WriteU8(0);
                }

                return Task.FromResult(writer.Build());
            },
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.StreamNotify, messageType);
                notifyHandler = handler;
                return new TestRegistration();
            });

        // Act
        var subscription = await stream.SubscribeAsync("stream://prod/*/*", (evt, cancellationToken) =>
        {
            received = evt;
            seenCancellationToken = cancellationToken;
            receivedTcs.TrySetResult(evt);
            return ValueTask.CompletedTask;
        });

        await Task.Delay(25);
        Assert.NotNull(notifyHandler);
        using var notification = new BinaryBufferWriter();
        notification.WriteU64(subscription.SubscriptionId);
        notification.WriteString("stream://prod/app/events");
        var json = System.Text.Encoding.UTF8.GetBytes("{\"event\":\"committed\",\"last_resource_offset\":19}");
        notification.WriteU32((uint)json.Length);
        notification.WriteBytes(json);
        notifyHandler!(notification.Build());

        var evt = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.NotNull(evt);
        Assert.Equal("stream://prod/app/events", evt!.Route);
        Assert.Equal((ulong)19, evt.CommitOffset);
        Assert.Same(received, evt);
        Assert.NotEqual(default, seenCancellationToken);
        Assert.False(seenCancellationToken.IsCancellationRequested);

        await subscription.DisposeAsync();
    }

    [Fact]
    public async Task should_return_committed_offset_given_append_response_when_appending_to_stream_session()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();

        var stream = new StreamClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            using var writer = new BinaryBufferWriter();
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
        var offset = await session.AppendAsync(12, "entry"u8.ToArray(), "meta"u8.ToArray());

        // Assert
        Assert.Equal((ulong)1234, offset);
        Assert.Equal(2, calls.Count);
        Assert.Equal(MessageTypes.StreamAppend, calls[1].MessageType);

        var reader = new BinaryBufferReader(calls[1].Payload);
        Assert.Equal((ulong)99, reader.ReadU64());
        Assert.Equal((ulong)12, reader.ReadU64());
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

            using var writer = new BinaryBufferWriter();
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
    public async Task should_ignore_commit_payload_given_success_response_when_committing()
    {
        // Arrange
        var stream = new StreamClient((messageType, payload, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (messageType == MessageTypes.StreamBegin)
            {
                writer.WriteU8(1);
                writer.WriteU64(44);
            }
            else if (messageType == MessageTypes.StreamCommit)
            {
                writer.WriteU32(0);
            }

            return Task.FromResult(writer.Build());
        });

        var session = await stream.BeginAsync("stream://prod/app/events");

        // Act / Assert
        await session.CommitAsync();
    }

    [Fact]
    public async Task should_encode_session_id_given_stream_session_when_rolling_back()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();

        var stream = new StreamClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            using var writer = new BinaryBufferWriter();
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
