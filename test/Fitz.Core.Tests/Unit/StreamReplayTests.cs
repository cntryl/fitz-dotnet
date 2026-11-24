using System.Text.Json;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class StreamReplayTests
{
    const string Route = "stream://realm/area/resource";
    static readonly TimeSpan Deadline = TimeSpan.FromSeconds(4);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ShouldReleaseFollowSubscriptionGivenCancellationOrDecodeFailure(bool cancel)
    {
        // Arrange
        var subscribes = 0;
        var unsubscribes = 0;
        await using var stream = new StreamClient((type, _, _) =>
        {
            switch (type)
            {
                case MessageTypes.StreamSubscribe:
                    subscribes++;
                    return Task.FromResult(SubscriptionReply());
                case MessageTypes.StreamUnsubscribe:
                    unsubscribes++;
                    throw new IOException("unsubscribe failed");
                default:
                    return Task.FromResult(cancel ? Page() : new byte[6]);
            }
        }, (_, _) => new TestRegistration());
        using var ct = new CancellationTokenSource(Deadline);
        await using var reader = stream.ReadAsync(Route, 0, StreamReadMode.Follow, ct: ct.Token).GetAsyncEnumerator(ct.Token);

        // Act
        if (cancel)
        {
            Assert.True(await reader.MoveNextAsync());
            var pending = reader.MoveNextAsync().AsTask();
            await ct.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        }
        else
        {
            await Assert.ThrowsAsync<StreamException>(() => reader.MoveNextAsync().AsTask());
        }

        // Assert
        Assert.Equal(1, unsubscribes);
        await using var sibling = await stream.SubscribeAsync(Route);
        Assert.Equal(2, subscribes);
    }

    [Fact]
    public async Task ShouldUseSelectorAxisGivenSharedReplayFixture()
    {
        // Arrange
        using var fixture = await LoadFixtureAsync();
        var axes = new Dictionary<string, ulong>(StringComparer.Ordinal)
        {
            ["resource"] = 11,
            ["area"] = 21,
            ["realm"] = 31,
            ["global"] = 41,
        };

        // Act

        foreach (var entry in fixture.RootElement.GetProperty("selectors").EnumerateArray())
        {
            var selector = entry.GetProperty("selector").GetString()!;
            var axis = entry.GetProperty("axis").GetString()!;
            await using var stream = new StreamClient((_, _, _) => Task.FromResult(Page(global: axis == "global")));
            var batch = await stream.ReadAsync(selector, 0, StreamReadMode.Replay).SingleAsync();

            // Assert
            Assert.True(batch.CaughtUp);
            Assert.Empty(batch.Records);
            Assert.Equal(axes[axis], batch.NextOffset);
        }

        foreach (var invalid in fixture.RootElement.GetProperty("invalidSelectors").EnumerateArray())
            Assert.False(RouteValidation.IsStreamSelector(invalid.GetString()!));
    }

    [Fact]
    public async Task ShouldRejectStalledGlobalCursorGivenRotatingContinuation()
    {
        // Arrange
        ulong reads = 0;
        await using var stream = new StreamClient((_, payload, _) =>
        {
            var request = new BinaryBufferReader(payload);
            request.ReadString();
            Assert.Equal(41UL, request.ReadU64());
            Assert.Equal(100UL, request.ReadU64());
            Assert.Equal((byte)0, request.ReadU8());
            Assert.Equal((byte)0, request.ReadU8());
            Assert.Equal(reads == 0 ? null : reads, StreamWireHelpers.ReadOptionalU64(request, "READ", "fingerprint"));
            Assert.Equal(reads == 0 ? null : 99UL, StreamWireHelpers.ReadOptionalU64(request, "READ", "watermark"));
            return Task.FromResult(Page(global: true, more: true, fingerprint: ++reads));
        });
        await using var batches = stream.ReadAsync("stream://**", 41, StreamReadMode.Replay).GetAsyncEnumerator();

        // Act
        Assert.True(await batches.MoveNextAsync());
        var error = await Assert.ThrowsAsync<StreamException>(() => batches.MoveNextAsync().AsTask());

        // Assert
        using var fixture = await LoadFixtureAsync();
        var stall = fixture.RootElement.GetProperty("progressInvariants").GetProperty("stallBound");
        Assert.Equal(stall.GetProperty("consecutiveHasMorePagesWithoutLogicalProgress").GetUInt64(), reads);
        Assert.Equal(stall.GetProperty("retryable").GetBoolean(), Retryability.IsRetryable(error));
    }

    [Fact]
    public async Task ShouldRememberCommitGivenNotificationDuringCatchUp()
    {
        // Arrange
        Action<byte[]>? notify = null;
        var requests = new List<ushort>();
        await using var stream = new StreamClient((type, _, _) =>
        {
            requests.Add(type);

            if (type == MessageTypes.StreamSubscribe)
                return Task.FromResult(SubscriptionReply());

            if (type == MessageTypes.StreamUnsubscribe)
                return Task.FromResult(new byte[] { 0 });

            if (requests.Count == 2)
            {
                using var frame = new BinaryBufferWriter();
                frame.WriteU64(7);
                frame.WriteString(Route);
                frame.WriteU32(0);
                notify!(frame.Build());
            }

            return Task.FromResult(Page());
        }, (_, handler) => { notify = handler; return new TestRegistration(); });
        using var timeout = new CancellationTokenSource(Deadline);

        // Act
        await using (var batches = stream.ReadAsync(Route, 0, StreamReadMode.Follow, ct: timeout.Token).GetAsyncEnumerator(timeout.Token))
        {
            Assert.True(await batches.MoveNextAsync());
            Assert.True(await batches.MoveNextAsync());
            Assert.Equal(11UL, batches.Current.FromOffset);
        }

        // Assert
        Assert.Equal([MessageTypes.StreamSubscribe, MessageTypes.StreamRead, MessageTypes.StreamRead, MessageTypes.StreamUnsubscribe], requests);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ShouldResumeWithFreshSnapshotGivenReconnectWithoutNotification(bool caughtUp)
    {
        // Arrange
        await using var first = new TestQueuedTransport();
        await using var second = new TestQueuedTransport();

        foreach (var transport in new[] { first, second })
            transport.QueueResponse(MessageTypes.StreamSubscribe, SubscriptionReply());

        first.QueueResponse(MessageTypes.StreamRead, Page(global: true, more: !caughtUp, fingerprint: 67));
        second.QueueResponse(MessageTypes.StreamRead, Page(global: true));
        second.QueueResponse(MessageTypes.StreamUnsubscribe, new byte[] { 0 });
        var tries = 0;
        var config = new ClientConfig(first.Url) { Reconnect = new ReconnectOptions(Backoff: TimeSpan.FromMilliseconds(1)) };
        await using var connection = new FitzConnection(config, () => ++tries == 1 ? first : second);
        await using var stream = new StreamClient(connection);
        using var timeout = new CancellationTokenSource(Deadline);
        await connection.ConnectAsync(timeout.Token);
        await using var reader = stream.ReadAsync("stream://**", 0, StreamReadMode.Follow, ct: timeout.Token).GetAsyncEnumerator(timeout.Token);
        Assert.True(await reader.MoveNextAsync());

        // Act
        first.QueueClosed();
        Assert.True(await reader.MoveNextAsync());

        // Assert
        Assert.Equal(41UL, reader.Current.FromOffset);
        var frame = Assert.Single(second.SentFrames.Select(bytes => FrameCodec.DecodeStrict(bytes)), frame => frame.MessageType == MessageTypes.StreamRead);
        var request = new BinaryBufferReader(frame.Payload);
        request.ReadString();
        Assert.Equal(41UL, request.ReadU64());
        Assert.Equal(100UL, request.ReadU64());
        Assert.Equal(new byte[4], request.ReadBytes(4));
    }

    static async Task<JsonDocument> LoadFixtureAsync() =>
        JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", "stream-read-conformance.json")));

    static byte[] SubscriptionReply()
    {
        using var reply = new BinaryBufferWriter();
        reply.WriteU8(0);
        reply.WriteU8(1);
        reply.WriteU64(7);
        return reply.Build();
    }

    internal static byte[] Page(bool global = false, bool more = false, ulong fingerprint = 0)
    {
        using var page = new BinaryBufferWriter();
        page.WriteU32(0);
        page.WriteU64(10);
        page.WriteU8(1);
        page.WriteU64(20);
        page.WriteU8(1);
        page.WriteU64(30);

        if (global)
        {
            page.WriteU8(1);
            page.WriteU64(40);
        }

        page.WriteU8(more ? (byte)1 : (byte)0);

        if (global)
        {
            page.WriteU8(1);
            page.WriteU64(fingerprint);
            page.WriteU8(1);
            page.WriteU64(99);
        }

        using var response = new BinaryBufferWriter();
        response.WriteU8(0);
        response.WriteU8(0);
        response.WriteLengthPrefixed(page.WrittenSpan);
        return response.Build();
    }
}
