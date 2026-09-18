using Cntryl.Fitz.Testing;
using Cntryl.Keys;

namespace Cntryl.Fitz.Extensions.Tests;

public sealed class KvDirectoryTests
{
    const string Route = "kv://test/widgets/tenant1";
    const string OtherRoute = "kv://test/widgets/tenant2";

    static readonly KvDirectoryIndex<Widget> ByNameV1 = new(
        "by_name", 1, static widget => [Normalize(widget.Name)]);
    static readonly KvDirectoryIndex<Widget> ByNameV2 = new(
        "by_name", 2, static widget => [Normalize(widget.Name)]);
    static readonly KvDirectoryIndex<Widget> ByPriorityName = new(
        "by_priority_name", 1, static widget => [widget.Priority, Normalize(widget.Name)]);
    static readonly KvDirectoryIndex<Widget> ByTag = KvDirectoryIndex.Many<Widget>(
        "by_tag", 1, static widget => widget.Tags?
            .Select(static tag => new LexKeyPart[] { Normalize(tag) }).ToArray() ?? []);
    static readonly KvDirectory<Widget, Guid> Directory = CreateDirectory(ByNameV1, ByPriorityName, ByTag);

    [Fact]
    public async Task ShouldReadEntityGivenInsertedValue()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var widget = new Widget(Guid.NewGuid(), "Alpha", 1);

        // Act
        await WriteAsync(client, transaction => Directory.InsertAsync(transaction, widget));

        // Assert
        Assert.Equal(widget, await Directory.GetAsync(client, Route, widget.Id));
    }

    [Fact]
    public async Task ShouldReadOneBoundedIndexRangeGivenMultipleBrokerPages()
    {
        // Arrange
        var client = new InMemoryKvClient(new InMemoryKvClientOptions { ScanPageSize = 1 });
        await WriteAsync(client, transaction => Directory.InsertAsync(
            transaction, new Widget(Guid.NewGuid(), "Beta", 2)));
        await WriteAsync(client, transaction => Directory.InsertAsync(
            transaction, new Widget(Guid.NewGuid(), "Alpha", 1)));
        client.ClearOperations();

        // Act
        var page = await Directory.QueryAsync(client, Route, ByNameV1.Query().Take(2));

        // Assert
        Assert.Equal(["Alpha", "Beta"], page.Items.Select(static widget => widget.Name));
        Assert.Null(page.NextCursor);
        Assert.All(client.Operations.Where(static operation => operation.Operation is KvTestOperation.Scan),
            static operation => Assert.Equal((uint)3, operation.ScanQuery!.Limit));
    }

    [Fact]
    public async Task ShouldKeepReadWorkBoundedGivenLargeDirectory()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using (var transaction = await client.BeginAsync(Route, KvDurability.Async, KvMode.ReadWrite))
        {
            for (var index = 0; index < 1_000; index++)
            {
                await Directory.InsertAsync(transaction,
                    new Widget(Guid.NewGuid(), $"Widget {index:D4}", index));
            }
            await transaction.CommitAsync();
        }
        client.ClearOperations();

        // Act
        var page = await Directory.QueryAsync(client, Route, ByNameV1.Query().Take(3));

        // Assert
        Assert.Equal(3, page.Items.Count);
        Assert.NotNull(page.NextCursor);
        Assert.DoesNotContain(client.Operations, static operation => operation.Operation is KvTestOperation.Get);
        var scan = Assert.Single(client.Operations, static operation => operation.Operation is KvTestOperation.Scan);
        Assert.Equal((uint)4, scan.ScanQuery!.Limit);
    }

    [Fact]
    public async Task ShouldResumeExclusivelyGivenDescendingCursor()
    {
        // Arrange
        var client = new InMemoryKvClient();
        foreach (var name in new[] { "Alpha", "Beta", "Gamma" })
        {
            await WriteAsync(client, transaction => Directory.InsertAsync(
                transaction, new Widget(Guid.NewGuid(), name, 1)));
        }

        // Act
        var first = await Directory.QueryAsync(client, Route, ByNameV1.Query().Descending().Take(2));
        var second = await Directory.QueryAsync(
            client, Route, ByNameV1.Query().Descending().Take(2).After(first.NextCursor));

        // Assert
        Assert.Equal(["Gamma", "Beta"], first.Items.Select(static widget => widget.Name));
        Assert.Equal(["Alpha"], second.Items.Select(static widget => widget.Name));
        Assert.Null(second.NextCursor);
    }

    [Fact]
    public async Task ShouldUseIdentityAsStableTieBreakerGivenDuplicateIndexValues()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var first = new Widget(Guid.Parse("00000000-0000-0000-0000-000000000001"), "Same", 1);
        var second = new Widget(Guid.Parse("00000000-0000-0000-0000-000000000002"), "Same", 1);
        await WriteAsync(client, transaction => Directory.InsertAsync(transaction, second));
        await WriteAsync(client, transaction => Directory.InsertAsync(transaction, first));

        // Act
        var page = await Directory.QueryAsync(client, Route, ByNameV1.Query());

        // Assert
        Assert.Equal([first.Id, second.Id], page.Items.Select(static widget => widget.Id));
    }

    [Fact]
    public async Task ShouldScanOnlyRequestedRangeGivenCompoundPrefix()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await WriteAsync(client, transaction => Directory.InsertAsync(
            transaction, new Widget(Guid.NewGuid(), "Low", 1)));
        await WriteAsync(client, transaction => Directory.InsertAsync(
            transaction, new Widget(Guid.NewGuid(), "High", 2)));

        // Act
        var page = await Directory.QueryAsync(
            client, Route, ByPriorityName.Query().WithPrefix([1]));

        // Assert
        Assert.Equal("Low", Assert.Single(page.Items).Name);
    }

    [Fact]
    public async Task ShouldReturnEntityFromEachMatchingTermGivenMultiEntryIndex()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var widget = new Widget(Guid.NewGuid(), "Alpha", 1, ["review", "admin"]);
        await WriteAsync(client, transaction => Directory.InsertAsync(transaction, widget));

        // Act
        var review = await Directory.QueryAsync(client, Route, ByTag.Query().WithPrefix(["REVIEW"]));
        var admin = await Directory.QueryAsync(client, Route, ByTag.Query().WithPrefix(["ADMIN"]));

        // Assert
        Assert.Equivalent(widget, Assert.Single(review.Items), strict: true);
        Assert.Equivalent(widget, Assert.Single(admin.Items), strict: true);
    }

    [Theory]
    [InlineData("different-index")]
    [InlineData("different-direction")]
    [InlineData("different-prefix")]
    [InlineData("different-route")]
    public async Task ShouldRejectCursorGivenDifferentQueryShape(string difference)
    {
        // Arrange
        var client = new InMemoryKvClient();
        await WriteAsync(client, transaction => Directory.InsertAsync(
            transaction, new Widget(Guid.NewGuid(), "Alpha", 1)));
        await WriteAsync(client, transaction => Directory.InsertAsync(
            transaction, new Widget(Guid.NewGuid(), "Beta", 2)));
        var first = await Directory.QueryAsync(client, Route, ByNameV1.Query().Take(1));
        var query = difference switch
        {
            "different-index" => ByPriorityName.Query().Take(1).After(first.NextCursor),
            "different-direction" => ByNameV1.Query().Descending().Take(1).After(first.NextCursor),
            "different-prefix" => ByNameV1.Query().WithPrefix(["A"]).Take(1).After(first.NextCursor),
            _ => ByNameV1.Query().Take(1).After(first.NextCursor),
        };

        // Act
        var error = await Assert.ThrowsAsync<KvDirectoryQueryException>(() => Directory.QueryAsync(
            client, difference == "different-route" ? OtherRoute : Route, query).AsTask());

        // Assert
        Assert.Equal(KvDirectoryQueryError.CursorMismatch, error.Kind);
    }

    [Fact]
    public async Task ShouldRejectCursorGivenMalformedOrOversizedValue()
    {
        // Arrange
        var client = new InMemoryKvClient();

        // Act
        var malformed = await Assert.ThrowsAsync<KvDirectoryQueryException>(() => Directory.QueryAsync(
            client, Route, ByNameV1.Query().After("not-base64")).AsTask());
        var oversized = await Assert.ThrowsAsync<KvDirectoryQueryException>(() => Directory.QueryAsync(
            client, Route, ByNameV1.Query().After(new string('A', 5000))).AsTask());

        // Assert
        Assert.Equal(KvDirectoryQueryError.InvalidCursor, malformed.Kind);
        Assert.Equal(KvDirectoryQueryError.InvalidCursor, oversized.Kind);
    }

    [Fact]
    public async Task ShouldRejectCursorGivenModifiedKeyOutsideIndexRange()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await WriteAsync(client, transaction => Directory.InsertAsync(
            transaction, new Widget(Guid.NewGuid(), "Alpha", 1)));
        await WriteAsync(client, transaction => Directory.InsertAsync(
            transaction, new Widget(Guid.NewGuid(), "Beta", 2)));
        var first = await Directory.QueryAsync(client, Route, ByNameV1.Query().Take(1));
        var payload = Convert.FromBase64String(first.NextCursor!);
        payload[21] = 0;

        // Act
        var error = await Assert.ThrowsAsync<KvDirectoryQueryException>(() => Directory.QueryAsync(
            client, Route, ByNameV1.Query().Take(1).After(Convert.ToBase64String(payload))).AsTask());

        // Assert
        Assert.Equal(KvDirectoryQueryError.InvalidCursor, error.Kind);
    }

    [Fact]
    public async Task ShouldRejectLimitGivenConfiguredMaximumExceeded()
    {
        // Arrange
        var client = new InMemoryKvClient();

        // Act
        var error = await Assert.ThrowsAsync<KvDirectoryQueryException>(() => Directory.QueryAsync(
            client, Route, ByNameV1.Query().Take(4)).AsTask());

        // Assert
        Assert.Equal(KvDirectoryQueryError.InvalidLimit, error.Kind);
    }

    [Fact]
    public async Task ShouldReplaceWithoutReadingGivenPreviousValue()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var previous = new Widget(Guid.NewGuid(), "Before", 1);
        var current = previous with { Name = "After" };
        await WriteAsync(client, transaction => Directory.InsertAsync(transaction, previous));
        client.ClearOperations();

        // Act
        await WriteAsync(client, transaction => Directory.ReplaceAsync(transaction, previous, current));

        // Assert
        Assert.DoesNotContain(client.Operations, static operation => operation.Operation is KvTestOperation.Get);
        Assert.Empty((await Directory.QueryAsync(
            client, Route, ByNameV1.Query().WithPrefix([Normalize("Before")]))).Items);
        Assert.Equal(current, Assert.Single((await Directory.QueryAsync(
            client, Route, ByNameV1.Query().WithPrefix([Normalize("After")]))).Items));
    }

    [Fact]
    public async Task ShouldReadBeforeWritingGivenConvenienceUpsert()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var previous = new Widget(Guid.NewGuid(), "Before", 1);
        await WriteAsync(client, transaction => Directory.InsertAsync(transaction, previous));
        client.ClearOperations();

        // Act
        await WriteAsync(client, transaction => Directory.UpsertAsync(
            transaction, previous with { Name = "After" }));

        // Assert
        _ = Assert.Single(client.Operations, static operation => operation.Operation is KvTestOperation.Get);
    }

    [Fact]
    public async Task ShouldDeleteWithoutReadingGivenPreviousValue()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var widget = new Widget(Guid.NewGuid(), "Alpha", 1);
        await WriteAsync(client, transaction => Directory.InsertAsync(transaction, widget));
        client.ClearOperations();

        // Act
        await WriteAsync(client, transaction => Directory.DeleteAsync(transaction, widget));

        // Assert
        Assert.DoesNotContain(client.Operations, static operation => operation.Operation is KvTestOperation.Get);
        Assert.Null(await Directory.GetAsync(client, Route, widget.Id));
        Assert.Empty((await Directory.QueryAsync(client, Route, ByNameV1.Query())).Items);
    }

    [Fact]
    public async Task ShouldBackfillInBoundedPagesGivenNewIndexGeneration()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var original = CreateDirectory(ByNameV1);
        foreach (var name in new[] { "Alpha", "Beta", "Gamma" })
        {
            await WriteAsync(client, transaction => original.InsertAsync(
                transaction, new Widget(Guid.NewGuid(), name, 1)));
        }
        var upgraded = CreateDirectory(ByNameV1, ByNameV2);

        // Act
        var first = await upgraded.BackfillAsync(client, Route, ByNameV2, limit: 2);
        var second = await upgraded.BackfillAsync(client, Route, ByNameV2, limit: 2, cursor: first.NextCursor);
        var page = await upgraded.QueryAsync(client, Route, ByNameV2.Query());

        // Assert
        Assert.Equal(2, first.Processed);
        Assert.NotNull(first.NextCursor);
        Assert.Equal(1, second.Processed);
        Assert.Null(second.NextCursor);
        Assert.Equal(["Alpha", "Beta", "Gamma"], page.Items.Select(static widget => widget.Name));
    }

    [Fact]
    public async Task ShouldRemoveOnlyObsoleteRowsGivenCompletedGenerationCutover()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var dualWrite = CreateDirectory(ByNameV1, ByNameV2);
        await WriteAsync(client, transaction => dualWrite.InsertAsync(
            transaction, new Widget(Guid.NewGuid(), "Alpha", 1)));
        var cutover = CreateDirectory(ByNameV2);

        // Act
        await WriteAsync(client, transaction => cutover.DeleteIndexGenerationAsync(transaction, ByNameV1));

        // Assert
        Assert.Empty((await dualWrite.QueryAsync(client, Route, ByNameV1.Query())).Items);
        Assert.Single((await cutover.QueryAsync(client, Route, ByNameV2.Query())).Items);
    }

    [Fact]
    public async Task ShouldRejectWriteGivenSerializedValueExceedsBound()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var widget = new Widget(Guid.NewGuid(), new string('x', 300), 1);

        // Act
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => WriteAsync(
            client, transaction => Directory.InsertAsync(transaction, widget)));

        // Assert
        Assert.Contains("exceeds 256 bytes", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ShouldRejectSchemaGivenDuplicateIndexNameAndGeneration()
    {
        // Arrange
        var duplicate = new KvDirectoryIndex<Widget>(
            ByNameV1.Name, ByNameV1.Generation, static widget => [widget.Name]);

        // Act
        // Assert
        _ = Assert.Throws<ArgumentException>(() => CreateDirectory(ByNameV1, duplicate));
    }

    [Fact]
    public async Task ShouldRejectWriteGivenIdentityHasNoKeyParts()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var directory = new KvDirectory<Widget, Guid>(
            "widgets",
            WidgetJsonContext.Default.Widget,
            static widget => widget.Id,
            static _ => [],
            [ByNameV1]);

        // Act
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => WriteAsync(
            client, transaction => directory.InsertAsync(
                transaction, new Widget(Guid.NewGuid(), "Alpha", 1))));

        // Assert
        Assert.Contains("at least one part", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShouldNotStageWriteGivenIndexExceedsBound()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var tooMany = KvDirectoryIndex.Many<Widget>(
            "too_many", 1, static _ => [["one"], ["two"]]);
        var directory = new KvDirectory<Widget, Guid>(
            "widgets",
            WidgetJsonContext.Default.Widget,
            static widget => widget.Id,
            static id => [id],
            [ByNameV1, tooMany],
            new KvDirectoryOptions { MaximumIndexEntriesPerEntity = 1 });
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async, KvMode.ReadWrite);
        client.ClearOperations();

        // Act
        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => directory.InsertAsync(
            transaction, new Widget(Guid.NewGuid(), "Alpha", 1)).AsTask());

        // Assert
        Assert.DoesNotContain(client.Operations, static operation =>
            operation.Operation is KvTestOperation.Insert or KvTestOperation.Put);
    }

    static KvDirectory<Widget, Guid> CreateDirectory(params KvDirectoryIndex<Widget>[] indexes) =>
        new(
            "widgets",
            WidgetJsonContext.Default.Widget,
            static widget => widget.Id,
            static id => [id],
            indexes,
            new KvDirectoryOptions
            {
                DefaultPageSize = 3,
                MaximumPageSize = 3,
                MaximumCursorBytes = 1024,
                MaximumSerializedValueBytes = 256,
                MaximumIndexGenerations = 4,
                MaximumIndexEntriesPerEntity = 8,
            });

    static string Normalize(string value) => value.ToUpperInvariant();

    static async Task WriteAsync(InMemoryKvClient client, Func<IKvTransaction, ValueTask> write)
    {
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async, KvMode.ReadWrite);
        await write(transaction);
        await transaction.CommitAsync();
    }
}
