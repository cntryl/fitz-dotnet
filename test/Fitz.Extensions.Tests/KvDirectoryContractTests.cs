using System.Text;
using System.Text.Json;
using Cntryl.Fitz.Testing;
using Cntryl.Keys;

namespace Cntryl.Fitz.Extensions.Tests;

public sealed class KvDirectoryContractTests
{
    const string Route = "kv://test/widgets/tenant1";

    static readonly KvDirectoryIndex<Widget> ByName = new(
        "by_name", 1, static widget => [widget.Name.ToUpperInvariant()]);
    static readonly KvDirectory<Widget, Guid> Directory = Create(ByName);

    [Fact]
    public async Task ShouldReturnNewestFirstGivenFirstDescendingPage()
    {
        // Arrange
        var client = new InMemoryKvClient();
        foreach (var name in new[] { "Alpha", "Beta", "Gamma" })
        {
            await WriteAsync(client, transaction => Directory.InsertAsync(
                transaction, new Widget(Guid.NewGuid(), name, 1)));
        }

        // Act
        var page = await Directory.QueryAsync(client, Route, ByName.Query().Descending().Take(3));

        // Assert
        Assert.Equal(["Gamma", "Beta", "Alpha"], page.Items.Select(static widget => widget.Name));
        Assert.Null(page.NextCursor);
    }

    [Fact]
    public async Task ShouldSendDirectionalBoundsAndExclusiveStartGivenDescendingCursor()
    {
        // Arrange
        var client = new InMemoryKvClient();
        foreach (var name in new[] { "Alpha", "Beta", "Gamma" })
        {
            await WriteAsync(client, transaction => Directory.InsertAsync(
                transaction, new Widget(Guid.NewGuid(), name, 1)));
        }
        var first = await Directory.QueryAsync(client, Route, ByName.Query().Descending().Take(1));
        client.ClearOperations();

        // Act
        await Directory.QueryAsync(client, Route, ByName.Query().Descending().Take(1).After(first.NextCursor));
        var scan = Assert.Single(client.Operations, static operation => operation.Operation is KvTestOperation.Scan);

        // Assert
        Assert.True(scan.ScanQuery!.Reverse);
        Assert.True(scan.ScanQuery.StartExclusive);
        Assert.True(scan.ScanQuery.StartKey!.Value.Span.SequenceCompareTo(scan.ScanQuery.EndKey!.Value.Span) > 0);
    }

    [Fact]
    public async Task ShouldWalkEveryDescendingPageGivenSmallBrokerPages()
    {
        // Arrange
        var client = new InMemoryKvClient(new InMemoryKvClientOptions { ScanPageSize = 1 });
        foreach (var name in new[] { "A", "B", "C", "D", "E" })
        {
            await WriteAsync(client, transaction => Directory.InsertAsync(
                transaction, new Widget(Guid.NewGuid(), name, 1)));
        }

        // Act
        var names = new List<string>();
        string? cursor = null;
        do
        {
            var page = await Directory.QueryAsync(client, Route, ByName.Query().Descending().Take(2).After(cursor));
            names.AddRange(page.Items.Select(static widget => widget.Name));
            cursor = page.NextCursor;
        }
        while (cursor is not null);

        // Assert
        Assert.Equal(["E", "D", "C", "B", "A"], names);
    }

    [Fact]
    public async Task ShouldDeleteByIdentityGivenEntityAndIdentityOfSameType()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var sameType = new KvDirectory<Widget, Widget>(
            "widgets",
            WidgetJsonContext.Default.Widget,
            static widget => widget,
            static widget => [widget.Id],
            [ByName]);
        var widget = new Widget(Guid.NewGuid(), "Alpha", 1);
        await WriteAsync(client, transaction => sameType.InsertAsync(transaction, widget));

        // Act
        await WriteAsync(client, transaction => sameType.DeleteByIdAsync(transaction, widget));

        // Assert
        Assert.Empty(client.Snapshot(Route));
    }

    [Fact]
    public async Task ShouldRemoveEveryRowGivenDeleteByIdentity()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var widget = new Widget(Guid.NewGuid(), "Alpha", 1);
        await WriteAsync(client, transaction => Directory.InsertAsync(transaction, widget));

        // Act
        await WriteAsync(client, transaction => Directory.DeleteByIdAsync(transaction, widget.Id));

        // Assert
        Assert.Empty(client.Snapshot(Route));
    }

    [Fact]
    public async Task ShouldIgnoreMissingIdentityGivenDeleteById()
    {
        // Arrange
        var client = new InMemoryKvClient();

        // Act
        await WriteAsync(client, transaction => Directory.DeleteByIdAsync(transaction, Guid.NewGuid()));

        // Assert
        Assert.Empty(client.Snapshot(Route));
    }

    [Fact]
    public async Task ShouldExplainRecoveryGivenStoredEntryThatNoLongerDeserializes()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var widget = new Widget(Guid.NewGuid(), "Alpha", 1);
        await WriteAsync(client, transaction => Directory.InsertAsync(transaction, widget));
        BreakStoredJson(client);

        // Act
        var delete = () => WriteAsync(client, transaction => Directory.DeleteByIdAsync(transaction, widget.Id));
        var upsert = () => WriteAsync(client, transaction => Directory.UpsertAsync(transaction, widget));

        // Assert
        var deleteError = await Assert.ThrowsAsync<InvalidOperationException>(delete);
        var upsertError = await Assert.ThrowsAsync<InvalidOperationException>(upsert);
        Assert.IsAssignableFrom<JsonException>(deleteError.InnerException);
        Assert.Contains("stored-entry reader", deleteError.Message, StringComparison.Ordinal);
        Assert.Contains("stored-entry reader", upsertError.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShouldDeleteByIdentityGivenReaderForOldShape()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var widget = new Widget(Guid.NewGuid(), "Alpha", 1);
        await WriteAsync(client, transaction => Directory.InsertAsync(transaction, widget));
        BreakStoredJson(client);

        // Act
        await WriteAsync(client, transaction => Directory.DeleteByIdAsync(
            transaction, widget.Id, static stored => LegacyReader(stored)));

        // Assert
        Assert.Empty(client.Snapshot(Route));
    }

    [Fact]
    public async Task ShouldReplaceIndexRowsGivenUpsertWithReaderForOldShape()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var widget = new Widget(Guid.NewGuid(), "Alpha", 1);
        await WriteAsync(client, transaction => Directory.InsertAsync(transaction, widget));
        BreakStoredJson(client);
        var renamed = widget with { Name = "Omega" };

        // Act
        await WriteAsync(client, transaction => Directory.UpsertAsync(
            transaction, renamed, static stored => LegacyReader(stored)));

        // Assert
        var page = await Directory.QueryAsync(client, Route, ByName.Query());
        Assert.Equal(["Omega"], page.Items.Select(static item => item.Name));
        Assert.Equal(2, client.Snapshot(Route).Count);
    }

    [Fact]
    public async Task ShouldLeaveOrphanedIndexRowGivenStalePreviousValue()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var stored = new Widget(Guid.NewGuid(), "Alpha", 1);
        await WriteAsync(client, transaction => Directory.InsertAsync(transaction, stored));
        var stale = stored with { Name = "Never stored" };

        // Act
        await WriteAsync(client, transaction => Directory.ReplaceAsync(transaction, stale, stored with { Name = "Beta" }));

        // Assert
        var page = await Directory.QueryAsync(client, Route, ByName.Query());
        Assert.Equal(["Alpha", "Beta"], page.Items.Select(static widget => widget.Name));
    }

    [Fact]
    public async Task ShouldNameParameterGivenInvalidLimitOrCursorOrIndex()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var unknown = new KvDirectoryIndex<Widget>("unknown", 1, static widget => [widget.Name]);

        // Act
        var limit = await Assert.ThrowsAsync<KvDirectoryQueryException>(() =>
            Directory.QueryPrimaryAsync(client, Route, 0).AsTask());
        var cursor = await Assert.ThrowsAsync<KvDirectoryQueryException>(() =>
            Directory.QueryPrimaryAsync(client, Route, 1, "not-base64").AsTask());
        var index = await Assert.ThrowsAsync<KvDirectoryQueryException>(() =>
            Directory.QueryAsync(client, Route, unknown.Query()).AsTask());

        // Assert
        Assert.Equal((KvDirectoryQueryError.InvalidLimit, "limit"), (limit.Kind, limit.ParamName));
        Assert.Equal((KvDirectoryQueryError.InvalidCursor, "cursor"), (cursor.Kind, cursor.ParamName));
        Assert.Equal((KvDirectoryQueryError.UnsupportedIndex, "query"), (index.Kind, index.ParamName));
    }

    [Fact]
    public void ShouldClassifyAsUnclassifiedGivenStandardConstructors()
    {
        // Arrange
        var inner = new InvalidOperationException();

        // Act
        var kinds = new[]
        {
            new KvDirectoryQueryException().Kind,
            new KvDirectoryQueryException("m").Kind,
            new KvDirectoryQueryException("m", inner).Kind,
        };

        // Assert
        Assert.All(kinds, static kind => Assert.Equal(KvDirectoryQueryError.Unclassified, kind));
    }

    [Fact]
    public void ShouldCarryExplicitKindAndParameterGivenClassifiedConstructor()
    {
        // Arrange
        var inner = new InvalidOperationException();

        // Act
        var error = new KvDirectoryQueryException(KvDirectoryQueryError.CursorMismatch, "mismatch", "cursor", inner);

        // Assert
        Assert.Equal(KvDirectoryQueryError.CursorMismatch, error.Kind);
        Assert.Equal("cursor", error.ParamName);
        Assert.Same(inner, error.InnerException);
    }

    [Fact]
    public async Task ShouldBehaveLikeConstructedDirectoryGivenBuilder()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var options = new KvDirectoryOptions { DefaultPageSize = 2, MaximumPageSize = 2 };
        var built = KvDirectory
            .Define("widgets", WidgetJsonContext.Default.Widget, static (Widget widget) => widget.Id, static (Guid id) => [id])
            .WithIndex(ByName)
            .WithOptions(options)
            .Build();
        var widget = new Widget(Guid.NewGuid(), "Alpha", 1);

        // Act
        await WriteAsync(client, transaction => built.InsertAsync(transaction, widget));
        var viaConstructed = await Directory.QueryAsync(client, Route, ByName.Query().Take(2));
        var tooMany = () => built.QueryAsync(client, Route, ByName.Query().Take(3)).AsTask();

        // Assert
        Assert.Equal(widget, Assert.Single(viaConstructed.Items));
        Assert.Equal(KvDirectoryQueryError.InvalidLimit, (await Assert.ThrowsAsync<KvDirectoryQueryException>(tooMany)).Kind);
    }

    [Fact]
    public void ShouldRejectDuplicateIndexGivenBuilder()
    {
        // Arrange
        var builder = KvDirectory
            .Define("widgets", WidgetJsonContext.Default.Widget, static (Widget widget) => widget.Id, static (Guid id) => [id])
            .WithIndex(ByName)
            .WithIndex(ByName);

        // Act
        var build = builder.Build;

        // Assert
        Assert.Throws<ArgumentException>(build);
    }

    static Widget LegacyReader(ReadOnlyMemory<byte> stored)
    {
        using var document = JsonDocument.Parse(stored);
        return new Widget(
            document.RootElement.GetProperty("LegacyId").GetGuid(),
            document.RootElement.GetProperty("LegacyName").GetString()!,
            1);
    }

    // Rewrites every stored row (primary and index) to a shape the current schema cannot read.
    static void BreakStoredJson(InMemoryKvClient client)
    {
        var rows = client.Snapshot(Route);
        foreach (var row in rows)
        {
            var original = JsonSerializer.Deserialize(row.Value.Span, WidgetJsonContext.Default.Widget)!;
            var legacy = $$"""{"Id":"no-longer-a-guid","LegacyId":"{{original.Id}}","LegacyName":"{{original.Name}}"}""";
            client.Seed(Route, row.Key, Encoding.UTF8.GetBytes(legacy));
        }
    }

    static KvDirectory<Widget, Guid> Create(params KvDirectoryIndex<Widget>[] indexes) =>
        new(
            "widgets",
            WidgetJsonContext.Default.Widget,
            static widget => widget.Id,
            static id => [id],
            indexes,
            new KvDirectoryOptions { DefaultPageSize = 3, MaximumPageSize = 3 });

    static async Task WriteAsync(InMemoryKvClient client, Func<IKvTransaction, ValueTask> write)
    {
        await using var transaction = await client.BeginAsync(Route, KvMode.ReadWrite);
        await write(transaction);
        await transaction.CommitAsync(KvDurability.Sync);
    }
}
