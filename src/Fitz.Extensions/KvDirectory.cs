using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Cntryl.Keys;

namespace Cntryl.Fitz.Extensions;

/// <summary>
/// A paginated, sortable, searchable KV-backed directory of composite-keyed values, built on
/// <see cref="LexKey"/> range encoding and <see cref="KvTransactionExtensions.ScanAllAsync"/>.
/// </summary>
/// <remarks>
/// <see cref="LexKey"/> is encode-only: nothing here recovers a field from a key's bytes. Any
/// field a caller needs back at read time — including the identifier a key was built from —
/// must round-trip through <typeparamref name="T"/>'s serialized value, not the key.
/// </remarks>
/// <typeparam name="T">The stored value type.</typeparam>
public sealed class KvDirectory<T>
{
    readonly JsonTypeInfo<T> _valueTypeInfo;
    readonly Func<T, string>? _searchText;
    readonly IReadOnlyDictionary<string, SortKeySelector<T>> _sortFields;

    /// <summary>Creates a directory over <typeparamref name="T"/>.</summary>
    /// <param name="valueTypeInfo">The source-generated contract used to serialize and deserialize stored values.</param>
    /// <param name="searchText">
    /// Extracts the text a <see cref="ListQuery.Search"/> filter matches against, or
    /// <see langword="null"/> if this directory does not support search.
    /// </param>
    /// <param name="sortFields">
    /// The <see cref="SortField.Field"/> names this directory can sort by, or <see langword="null"/>
    /// if it does not support sort.
    /// </param>
    public KvDirectory(
        JsonTypeInfo<T> valueTypeInfo,
        Func<T, string>? searchText = null,
        IReadOnlyDictionary<string, SortKeySelector<T>>? sortFields = null)
    {
        ArgumentNullException.ThrowIfNull(valueTypeInfo);
        _valueTypeInfo = valueTypeInfo;
        _searchText = searchText;
        _sortFields = sortFields ?? new Dictionary<string, SortKeySelector<T>>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>Stores one value, replacing any value already at <paramref name="key"/>.</summary>
    /// <param name="transaction">An open, writable transaction — typically a Portia projector's own transaction.</param>
    /// <param name="key">The exact composite key, e.g. <c>["team", teamId]</c>.</param>
    /// <param name="value">The value to store.</param>
    /// <param name="ct">A token that can cancel the operation.</param>
    public async ValueTask PutAsync(
        IKvTransaction transaction,
        LexKeyPart[] key,
        T value,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(key);
        var encodedKey = LexKey.EncodeComposite(key).AsMemory();
        var encodedValue = JsonSerializer.SerializeToUtf8Bytes(value, _valueTypeInfo);
        await transaction.PutAsync(encodedKey, encodedValue, ct).ConfigureAwait(false);
    }

    /// <summary>Removes the value at <paramref name="key"/>, if any.</summary>
    /// <param name="transaction">An open, writable transaction — typically a Portia projector's own transaction.</param>
    /// <param name="key">The exact composite key, e.g. <c>["team", teamId]</c>.</param>
    /// <param name="ct">A token that can cancel the operation.</param>
    public async ValueTask DeleteAsync(IKvTransaction transaction, LexKeyPart[] key, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(key);
        var encodedKey = LexKey.EncodeComposite(key).AsMemory();
        await transaction.DeleteAsync(encodedKey, ct).ConfigureAwait(false);
    }

    /// <summary>Reads one value by its exact key, independent of any projector's workload scope.</summary>
    /// <param name="client">The KV client to read through.</param>
    /// <param name="route">The exact KV route.</param>
    /// <param name="key">The exact composite key, e.g. <c>["team", teamId]</c>.</param>
    /// <param name="ct">A token that can cancel the operation.</param>
    /// <returns>The stored value, or <see langword="default"/> when no value exists at <paramref name="key"/>.</returns>
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task",
        Justification = "The await-using declaration must retain the strongly typed transaction for GetAsync.")]
    public async ValueTask<T?> GetAsync(
        IKvClient client,
        string route,
        LexKeyPart[] key,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(key);
        var encodedKey = LexKey.EncodeComposite(key).AsMemory();
        await using var transaction = await client.BeginAsync(route, KvDurability.Async, KvMode.ReadOnly, ct)
            .ConfigureAwait(false);
        var result = await transaction.GetAsync(encodedKey, ct).ConfigureAwait(false);
        return result.Found ? Deserialize(result.Value!.Value) : default;
    }

    /// <summary>
    /// Lists every value whose key starts with <paramref name="prefix"/>, paginated, optionally
    /// filtered and sorted.
    /// </summary>
    /// <param name="client">The KV client to read through.</param>
    /// <param name="route">The exact KV route.</param>
    /// <param name="prefix">
    /// The composite key prefix to list, e.g. <c>["team-member", teamId]</c> to list one team's
    /// members. Must be a strict prefix of the full keys written by <see cref="PutAsync"/> — never
    /// include the item's own identifier.
    /// </param>
    /// <param name="query">The normalized list query.</param>
    /// <param name="ct">A token that can cancel the operation.</param>
    /// <returns>The matching page.</returns>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="query"/> requests search or sort this directory was not configured for.
    /// </exception>
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task",
        Justification = "The await-using declaration must retain the strongly typed transaction for ListAsync.")]
    public async ValueTask<Page<T>> ListAsync(
        IKvClient client,
        string route,
        LexKeyPart[] prefix,
        ListQuery query,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentNullException.ThrowIfNull(query);
        if (query.Search is not null && _searchText is null)
        {
            throw new InvalidOperationException(
                "This directory has no search selector configured; construct it with a searchText " +
                "delegate to support ListQuery.Search.");
        }

        foreach (var field in query.Sort)
        {
            if (!_sortFields.ContainsKey(field.Field))
            {
                throw new InvalidOperationException(
                    $"This directory has no sort selector configured for field '{field.Field}'.");
            }
        }

        var rangeStart = LexKey.EncodeFirst(prefix).AsMemory();
        var rangeEnd = LexKey.EncodeLast(prefix).AsMemory();
        await using var transaction = await client.BeginAsync(route, KvDurability.Async, KvMode.ReadOnly, ct)
            .ConfigureAwait(false);

        return query.Sort.Count == 0
            ? await ListByKeyOrderAsync(transaction, rangeStart, rangeEnd, query, ct).ConfigureAwait(false)
            : await ListSortedAsync(transaction, rangeStart, rangeEnd, query, ct).ConfigureAwait(false);
    }

    async ValueTask<Page<T>> ListByKeyOrderAsync(
        IKvTransaction transaction,
        ReadOnlyMemory<byte> rangeStart,
        ReadOnlyMemory<byte> rangeEnd,
        ListQuery query,
        CancellationToken ct)
    {
        var startKey = DecodeKeyCursor(query.Cursor) ?? rangeStart;
        // Collect one extra qualifying match beyond the requested limit so "is there a next page"
        // is answered by an actual match, not just by this page happening to fill exactly.
        var matches = new List<(T Value, ReadOnlyMemory<byte> Key)>();
        var scanQuery = new KvScanQuery(startKey, rangeEnd, Limit: (ulong)(query.Limit + 1));

        await foreach (var pair in transaction.ScanAllAsync(scanQuery, ct).ConfigureAwait(false))
        {
            var value = Deserialize(pair.Value);
            if (query.Search is not null && !_searchText!(value).Contains(query.Search, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            matches.Add((value, pair.Key));
            if (matches.Count > query.Limit)
            {
                break;
            }
        }

        var hasMore = matches.Count > query.Limit;
        var items = matches.Take(query.Limit).Select(static match => match.Value).ToArray();
        var nextCursor = hasMore ? EncodeKeyCursor(NextKeyAfter(matches[query.Limit - 1].Key.Span)) : null;
        return new Page<T>(items, nextCursor);
    }

    async ValueTask<Page<T>> ListSortedAsync(
        IKvTransaction transaction,
        ReadOnlyMemory<byte> rangeStart,
        ReadOnlyMemory<byte> rangeEnd,
        ListQuery query,
        CancellationToken ct)
    {
        var all = new List<T>();
        var scanQuery = new KvScanQuery(rangeStart, rangeEnd);
        await foreach (var pair in transaction.ScanAllAsync(scanQuery, ct).ConfigureAwait(false))
        {
            var value = Deserialize(pair.Value);
            if (query.Search is null || _searchText!(value).Contains(query.Search, StringComparison.OrdinalIgnoreCase))
            {
                all.Add(value);
            }
        }

        var ordered = Sort(all, query.Sort);
        var offset = DecodeOffsetCursor(query.Cursor);
        var items = ordered.Skip(offset).Take(query.Limit).ToArray();
        var nextCursor = offset + items.Length < ordered.Count ? EncodeOffsetCursor(offset + items.Length) : null;
        return new Page<T>(items, nextCursor);
    }

    List<T> Sort(List<T> items, IReadOnlyList<SortField> sort)
    {
        IOrderedEnumerable<T>? ordered = null;
        foreach (var field in sort)
        {
            var selector = _sortFields[field.Field];
            ordered = ordered is null
                ? field.Descending ? items.OrderByDescending(v => selector(v)) : items.OrderBy(v => selector(v))
                : field.Descending ? ordered.ThenByDescending(v => selector(v)) : ordered.ThenBy(v => selector(v));
        }

        return ordered is null ? items : [.. ordered];
    }

    T Deserialize(ReadOnlyMemory<byte> value) =>
        JsonSerializer.Deserialize(value.Span, _valueTypeInfo)
        ?? throw new InvalidOperationException("The stored directory entry could not be deserialized.");

    // A ternary here (`cursor is null ? null : ...`) resolves its common type through
    // ReadOnlyMemory<byte>'s implicit byte[]? conversion, turning `null` into an empty memory
    // instead of "no value" — explicit branches avoid that ambiguous inference entirely.
    static ReadOnlyMemory<byte>? DecodeKeyCursor(string? cursor)
    {
        if (cursor is null)
        {
            return null;
        }

        return Convert.FromBase64String(cursor);
    }

    static string EncodeKeyCursor(ReadOnlyMemory<byte> key) => Convert.ToBase64String(key.Span);

    static int DecodeOffsetCursor(string? cursor) =>
        cursor is null ? 0 : int.Parse(cursor, System.Globalization.CultureInfo.InvariantCulture);

    static string EncodeOffsetCursor(int offset) => offset.ToString(System.Globalization.CultureInfo.InvariantCulture);

    static ReadOnlyMemory<byte> NextKeyAfter(ReadOnlySpan<byte> key)
    {
        var next = new byte[key.Length + 1];
        key.CopyTo(next);
        return next;
    }
}
