namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed class StreamBoundariesTests
{
    [Fact]
    public async Task ShouldPreserveEventMetadataGivenEmptyAndAbsentValuesWhenReplayed()
    {
        // Arrange
        var transport = IntegrationFixture.GetConformanceTransport();
        var authMode = IntegrationFixture.GetConformanceAuthMode();
        await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("stream");
        await using var session = await client.Stream.BeginAsync(route);

        // Act
        await session.AppendAsync(0, "empty"u8.ToArray(), ReadOnlyMemory<byte>.Empty);
        await session.AppendAsync(1, "absent"u8.ToArray());
        await session.CommitAsync(StreamCommitMode.Sync);
        var page = await client.Stream.ReadPageAsync(route, 0, 10);

        // Assert
        Assert.Equal(2, page.Items.Count);
        var empty = Assert.IsType<StreamRecord>(page.Items[0].Record);
        Assert.True(empty.Metadata.HasValue);
        Assert.True(empty.Metadata.GetValueOrDefault().IsEmpty);
        var absent = Assert.IsType<StreamRecord>(page.Items[1].Record);
        Assert.Null(absent.Metadata);
    }

    [Fact]
    public async Task ShouldReplayStreamEventsGivenBoundarySizedPayloadsWhenRouteIsLongest()
    {
        // Arrange
        var transport = IntegrationFixture.GetConformanceTransport();
        var authMode = IntegrationFixture.GetConformanceAuthMode();
        await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
        await client.ConnectAsync();
        var prefix = IntegrationFixture.CreateUniqueRoute("stream");
        var route = prefix + new string('r', 4096 - prefix.Length);
        var metadata = "0123456789abcdef"u8.ToArray();
        var body = new byte[400 * 1024 - metadata.Length];
        Array.Fill(body, (byte)42);

        // Act
        for (ulong offset = 0; offset < 3; offset++)
        {
            await using var session = await client.Stream.BeginAsync(route);
            if (offset == 0)
            {
                var oversized = new byte[body.Length + 1];
                var error = await Assert.ThrowsAsync<StreamException>(() =>
                    session.AppendAsync(0, oversized, metadata));
                Assert.Equal((uint)2008, error.DomainCode);
                Assert.Equal((byte)0, error.ErrorEffect);
            }
            Assert.Equal(offset, await session.AppendAsync(offset, body, metadata));
            await session.CommitAsync(StreamCommitMode.Sync);
        }

        var records = new List<StreamRecord>();
        ulong next = 0;
        var pages = 0;
        while (true)
        {
            var page = await client.Stream.ReadPageAsync(route, next, 10);
            pages++;
            foreach (var item in page.Items)
            {
                records.Add(Assert.IsType<StreamRecord>(item.Record));
            }
            if (!page.Cursor.HasMore)
            {
                break;
            }
            Assert.True(page.Cursor.LastResourceOffset >= next);
            next = page.Cursor.LastResourceOffset + 1;
        }

        // Assert
        Assert.True(pages > 1);
        Assert.Equal(3, records.Count);
        for (var offset = 0; offset < records.Count; offset++)
        {
            Assert.Equal(route, records[offset].Route);
            Assert.Equal((ulong)offset, records[offset].Offset);
            Assert.Equal(body, records[offset].Body.ToArray());
            Assert.True(records[offset].Metadata.HasValue);
            Assert.Equal(metadata, records[offset].Metadata.GetValueOrDefault().ToArray());
        }
    }
}
