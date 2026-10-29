namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed partial class ConformanceSmokeTests
{
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
            var initial = await client.Stream.ReadPageAsync("stream://**", 0, 1);
            var next = initial.Cursor.CapturedWatermark ?? throw new InvalidOperationException("Missing global watermark");
            foreach (var route in new[] { firstRoute, secondRoute })
            {
                await using var session = await client.Stream.BeginAsync(route);
                await session.AppendAsync(0, "skip"u8.ToArray(), discriminator: "skip");
                await session.AppendAsync(1, "keep"u8.ToArray(), discriminator: discriminator);
                await session.CommitAsync(StreamCommitMode.Sync);
            }

            var records = new List<StreamRecord>();
            ulong? fingerprint = null;
            ulong? watermark = null;
            var filteredOnly = false;
            var pages = 0;
            while (true)
            {
                var page = await client.Stream.ReadPageAsync("stream://**", next, 1, filter, cursorFingerprint: fingerprint, capturedWatermark: watermark);
                pages++;
                filteredOnly |= page.Items.Count > 0 && page.Items.All(item => item.Record is null);
                records.AddRange(page.Items.Where(item => item.Record is not null).Select(item => item.Record!));
                if (!page.Cursor.HasMore)
                {
                    break;
                }
                var last = page.Cursor.LastGlobalOffset ?? throw new InvalidOperationException("Missing global cursor offset");
                Assert.True(last >= next);
                next = last + 1;
                fingerprint = page.Cursor.CursorFingerprint;
                watermark = page.Cursor.CapturedWatermark;
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
        catch (Exception error)
        {
            evidence.Add($"global filtered replay failed: {error.GetType().Name}: {error.Message}");
            return Result("CS-017", transport, authMode, "fail", stopwatch.ElapsedMilliseconds, evidence, error.Message);
        }
    }
}
