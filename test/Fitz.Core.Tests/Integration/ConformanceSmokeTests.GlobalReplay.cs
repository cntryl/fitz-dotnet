namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed partial class ConformanceSmokeTests
{
    [Fact]
    public async Task ShouldFollowFirstAppendGivenInitiallyEmptyResource()
    {
        // Arrange
        var transport = IntegrationFixture.GetConformanceTransport();
        var authMode = IntegrationFixture.GetConformanceAuthMode();
        await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var ct = deadline.Token;
        await client.ConnectAsync(ct);
        var route = IntegrationFixture.CreateUniqueRoute("stream");
        await using var batches = client.Stream.ReadAsync(route, 0, StreamReadMode.Follow, ct: ct).GetAsyncEnumerator(ct);

        // Act
        Assert.True(await batches.MoveNextAsync());
        var empty = batches.Current;
        await using var session = await client.Stream.BeginAsync(route, ct: ct);
        await session.AppendAsync(0, "first"u8.ToArray(), ct: ct);
        await session.CommitAsync(StreamCommitMode.Sync, ct);
        Assert.True(await batches.MoveNextAsync());

        // Assert
        Assert.Empty(empty.Items);
        Assert.True(empty.CaughtUp);
        Assert.Equal(0UL, empty.NextOffset);
        Assert.Equal(0UL, Assert.Single(batches.Current.Records).Offset);
        Assert.Equal(1UL, batches.Current.NextOffset);
    }

    [Fact]
    public async Task ShouldReplayGlobalStreamGivenFilteredMultiPageSelectionWhenConnected()
    {
        // Arrange
        var transport = IntegrationFixture.GetConformanceTransport();
        var authMode = IntegrationFixture.GetConformanceAuthMode();

        // Act
        var result = await RunCs017GlobalFilteredMultiPageReplay(transport, authMode);

        // Assert
        Assert.Equal("pass", result.Verdict);
    }

    static async Task<ScenarioResult> RunCs017GlobalFilteredMultiPageReplay(string transport, string authMode)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
            await client.ConnectAsync();
            var firstRoute = IntegrationFixture.CreateUniqueRoute("stream");
            var secondRoute = firstRoute.Replace("stream://conformance-realm/", "stream://other-realm/", StringComparison.Ordinal);
            var discriminator = $"match-{firstRoute}";
            var filter = new StreamFilterSet
            {
                Clauses = [new StreamFilterClause { Kind = StreamFilterClauseKind.Equals, Value = discriminator }],
            };
            var tail = await client.Stream.ReadAsync("stream://**", 0, StreamReadMode.Replay).LastAsync();

            foreach (var route in new[] { firstRoute, secondRoute })
            {
                await using var session = await client.Stream.BeginAsync(route);
                await session.AppendAsync(0, "skip"u8.ToArray(), discriminator: "skip");
                await session.AppendAsync(1, "keep"u8.ToArray(), discriminator: discriminator);
                await session.CommitAsync(StreamCommitMode.Sync);
            }


            var records = new List<StreamRecord>();
            var filteredOnly = false;
            var pages = 0;
            var next = tail.NextOffset;
            var options = new StreamReadOptions { BatchSize = 1, Filter = filter };

            await foreach (var batch in client.Stream.ReadAsync("stream://**", next, StreamReadMode.Replay, options))
            {
                Assert.Equal(next, batch.FromOffset);
                filteredOnly |= batch.Items.Count > 0 && batch.Records.Count == 0;
                records.AddRange(batch.Records);
                next = batch.NextOffset;
                pages++;
            }


            Assert.True(pages > 1);
            Assert.True(filteredOnly);
            Assert.Equal(new[] { firstRoute, secondRoute }, records.Select(record => record.Route));
            Assert.Equal(2, records.Count);
            Assert.True(records[0].GlobalOffset.HasValue && records[1].GlobalOffset.HasValue);
            Assert.True(records[0].GlobalOffset < records[1].GlobalOffset);
            evidence.Add($"global filtered replay yielded {pages} pages across two realms");
            evidence.Add("filtered-only pages advanced the global cursor through the captured tail");
            return Result("CS-017", transport, authMode, "pass", stopwatch.ElapsedMilliseconds, evidence);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            evidence.Add($"global filtered replay failed: {error.GetType().Name}: {error.Message}");
            return Result("CS-017", transport, authMode, "fail", stopwatch.ElapsedMilliseconds, evidence, error.Message);
        }
    }
}
