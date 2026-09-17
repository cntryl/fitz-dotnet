using Cntryl.Fitz.Testing;
using Cntryl.Keys;

namespace Cntryl.Fitz.Extensions.Tests;

public sealed class KvDirectoryTests
{
    const string Route = "kv://test/widgets/tenant1";

    static readonly KvDirectory<Widget> SearchableSortable = new(
        WidgetJsonContext.Default.Widget,
        searchText: static widget => widget.Name,
        sortFields: new Dictionary<string, SortKeySelector<Widget>>(StringComparer.OrdinalIgnoreCase)
        {
            ["name"] = static widget => widget.Name,
            ["priority"] = static widget => widget.Priority,
        });

    static readonly KvDirectory<Widget> Plain = new(WidgetJsonContext.Default.Widget);

    [Fact]
    public async Task ShouldReturnTheStoredValueGivenAPreviousPut()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var id = Guid.NewGuid();
        var widget = new Widget(id, "Alpha", 1);
        await PutAsync(client, ["widgets", id], widget);

        // Act
        var result = await Plain.GetAsync(client, Route, ["widgets", id], CancellationToken.None);

        // Assert
        Assert.Equal(widget, result);
    }

    [Fact]
    public async Task ShouldReturnDefaultGivenNoSuchKey()
    {
        // Arrange
        var client = new InMemoryKvClient();

        // Act
        var result = await Plain.GetAsync(client, Route, ["widgets", Guid.NewGuid()], CancellationToken.None);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ShouldRemoveTheStoredValueGivenADeletedKey()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var id = Guid.NewGuid();
        await PutAsync(client, ["widgets", id], new Widget(id, "Alpha", 1));
        await DeleteAsync(client, ["widgets", id]);

        // Act
        var result = await Plain.GetAsync(client, Route, ["widgets", id], CancellationToken.None);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ShouldReturnEveryItemGivenNoFilter()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Alpha", 1));
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Beta", 2));

        // Act
        var page = await Plain.ListAsync(client, Route, ["widgets"], ListQuery.From(null, null, null, null));

        // Assert
        Assert.Equal(2, page.Items.Count);
        Assert.Null(page.NextCursor);
    }

    // Regression test: a naive lower-bound-less scan would include a partition that sorts BEFORE
    // the requested one. "aaa" is chosen specifically because it sorts before "bbb" — the exact
    // shape that let a missing lower bound leak a neighboring partition's entries through.
    [Fact]
    public async Task ShouldOnlyReturnItemsInTheRequestedPartitionGivenAnEarlierSortingSibling()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var wanted = Guid.NewGuid();
        await PutAsync(client, ["widgets", "bbb", wanted], new Widget(wanted, "Wanted", 1));
        await PutAsync(client, ["widgets", "aaa", Guid.NewGuid()], new Widget(Guid.NewGuid(), "NotWanted", 1));

        // Act
        var page = await Plain.ListAsync(
            client, Route, ["widgets", "bbb"], ListQuery.From(null, null, null, null));

        // Assert
        var item = Assert.Single(page.Items);
        Assert.Equal(wanted, item.Id);
    }

    // Regression test: a later-sorting sibling must also stay excluded — the upper bound side.
    [Fact]
    public async Task ShouldOnlyReturnItemsInTheRequestedPartitionGivenALaterSortingSibling()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var wanted = Guid.NewGuid();
        await PutAsync(client, ["widgets", "bbb", wanted], new Widget(wanted, "Wanted", 1));
        await PutAsync(client, ["widgets", "ccc", Guid.NewGuid()], new Widget(Guid.NewGuid(), "NotWanted", 1));

        // Act
        var page = await Plain.ListAsync(
            client, Route, ["widgets", "bbb"], ListQuery.From(null, null, null, null));

        // Assert
        var item = Assert.Single(page.Items);
        Assert.Equal(wanted, item.Id);
    }

    // Regression test: NextCursor must not be set just because a page happened to fill exactly.
    [Fact]
    public async Task ShouldPaginateWithoutAnExtraEmptyPageGivenExactlyOneMoreItem()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Alpha", 1));
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Beta", 2));

        // Act
        var first = await Plain.ListAsync(client, Route, ["widgets"], ListQuery.From(1, null, null, null));
        var second = await Plain.ListAsync(
            client, Route, ["widgets"], ListQuery.From(1, first.NextCursor, null, null));

        // Assert
        Assert.Single(first.Items);
        Assert.NotNull(first.NextCursor);
        Assert.Single(second.Items);
        Assert.Null(second.NextCursor);
    }

    [Fact]
    public async Task ShouldThrowGivenLimitIsZero()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Alpha", 1));
        var query = new ListQuery(0, null, null, []);

        // Act
        // Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Plain.ListAsync(client, Route, ["widgets"], query).AsTask());
    }

    [Fact]
    public async Task ShouldThrowGivenLimitIsNegative()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var query = new ListQuery(-1, null, null, []);

        // Act
        // Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Plain.ListAsync(client, Route, ["widgets"], query).AsTask());
    }

    [Fact]
    public async Task ShouldPaginateASortedQueryGivenTwoPages()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Alpha", 1));
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Beta", 2));
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Gamma", 3));

        // Act
        var first = await SearchableSortable.ListAsync(
            client, Route, ["widgets"], ListQuery.From(1, null, null, "name:asc"));
        var second = await SearchableSortable.ListAsync(
            client, Route, ["widgets"], ListQuery.From(1, first.NextCursor, null, "name:asc"));
        var third = await SearchableSortable.ListAsync(
            client, Route, ["widgets"], ListQuery.From(1, second.NextCursor, null, "name:asc"));

        // Assert
        Assert.Equal(["Alpha"], first.Items.Select(w => w.Name));
        Assert.Equal(["Beta"], second.Items.Select(w => w.Name));
        Assert.Equal(["Gamma"], third.Items.Select(w => w.Name));
        Assert.Null(third.NextCursor);
    }

    [Fact]
    public async Task ShouldPaginateSearchResultsGivenMatchesSparseAcrossPages()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Reviewers", 1));
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Administrators", 1));
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Reviewing", 1));

        // Act
        var first = await SearchableSortable.ListAsync(
            client, Route, ["widgets"], ListQuery.From(1, null, "review", null));
        var second = await SearchableSortable.ListAsync(
            client, Route, ["widgets"], ListQuery.From(1, first.NextCursor, "review", null));

        // Assert
        Assert.Single(first.Items);
        Assert.NotNull(first.NextCursor);
        Assert.Single(second.Items);
        Assert.Null(second.NextCursor);
        Assert.Equal(
            ["Reviewers", "Reviewing"],
            first.Items.Concat(second.Items).Select(w => w.Name).Order());
    }

    [Fact]
    public async Task ShouldFilterBySearchGivenAMatchingSubstring()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Reviewers", 1));
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Administrators", 1));

        // Act
        var page = await SearchableSortable.ListAsync(
            client, Route, ["widgets"], ListQuery.From(null, null, "review", null));

        // Assert
        var item = Assert.Single(page.Items);
        Assert.Equal("Reviewers", item.Name);
    }

    [Fact]
    public async Task ShouldSortDescendingGivenASingleSortField()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Alpha", 1));
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Beta", 2));

        // Act
        var page = await SearchableSortable.ListAsync(
            client, Route, ["widgets"], ListQuery.From(null, null, null, "name:desc"));

        // Assert
        Assert.Equal(["Beta", "Alpha"], page.Items.Select(w => w.Name));
    }

    [Fact]
    public async Task ShouldSortByMultipleFieldsGivenACompoundSortExpression()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Alpha", 2));
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Alpha", 1));
        await PutAsync(client, ["widgets", Guid.NewGuid()], new Widget(Guid.NewGuid(), "Beta", 1));

        // Act
        var page = await SearchableSortable.ListAsync(
            client, Route, ["widgets"], ListQuery.From(null, null, null, "name:asc,priority:asc"));

        // Assert
        Assert.Equal(
            [("Alpha", 1), ("Alpha", 2), ("Beta", 1)],
            page.Items.Select(w => (w.Name, w.Priority)));
    }

    [Fact]
    public async Task ShouldThrowGivenSearchWithoutASearchSelector()
    {
        // Arrange
        var client = new InMemoryKvClient();

        // Act
        // Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Plain.ListAsync(client, Route, ["widgets"], ListQuery.From(null, null, "anything", null)).AsTask());
    }

    [Fact]
    public async Task ShouldThrowGivenAnUnconfiguredSortField()
    {
        // Arrange
        var client = new InMemoryKvClient();

        // Act
        // Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            SearchableSortable.ListAsync(client, Route, ["widgets"], ListQuery.From(null, null, null, "unknown"))
                .AsTask());
    }

    static async Task PutAsync(InMemoryKvClient client, LexKeyPart[] key, Widget widget)
    {
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async, KvMode.ReadWrite);
        await Plain.PutAsync(transaction, key, widget, CancellationToken.None);
        await transaction.CommitAsync();
    }

    static async Task DeleteAsync(InMemoryKvClient client, LexKeyPart[] key)
    {
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async, KvMode.ReadWrite);
        await Plain.DeleteAsync(transaction, key, CancellationToken.None);
        await transaction.CommitAsync();
    }
}
