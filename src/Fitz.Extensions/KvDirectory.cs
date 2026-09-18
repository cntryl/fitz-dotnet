using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Cntryl.Keys;

namespace Cntryl.Fitz.Extensions;

/// <summary>
/// A bounded, index-backed KV directory. Primary records have stable keys; independently versioned
/// covering indexes provide ordered queries without materializing or sorting the directory.
/// </summary>
/// <typeparam name="T">Stored entity type.</typeparam>
/// <typeparam name="TKey">Strongly typed entity identity.</typeparam>
public sealed class KvDirectory<T, TKey>
{
    const byte CursorVersion = 1;
    const int FingerprintLength = 16;
    readonly string _name;
    readonly JsonTypeInfo<T> _valueTypeInfo;
    readonly Func<T, TKey> _identity;
    readonly Func<TKey, LexKeyPart[]> _identityKey;
    readonly KvDirectoryIndex<T>[] _indexes;
    readonly KvDirectoryOptions _options;

    /// <summary>Creates an immutable directory schema.</summary>
    public KvDirectory(
        string name,
        JsonTypeInfo<T> valueTypeInfo,
        Func<T, TKey> identity,
        Func<TKey, LexKeyPart[]> identityKey,
        IReadOnlyList<KvDirectoryIndex<T>> indexes,
        KvDirectoryOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(valueTypeInfo);
        ArgumentNullException.ThrowIfNull(identity);
        ArgumentNullException.ThrowIfNull(identityKey);
        ArgumentNullException.ThrowIfNull(indexes);
        _options = options ?? new KvDirectoryOptions();
        if (_options.MaximumPageSize <= 0 || _options.MaximumPageSize == int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(options), "MaximumPageSize must be between 1 and Int32.MaxValue - 1.");
        if (_options.DefaultPageSize <= 0 || _options.DefaultPageSize > _options.MaximumPageSize)
            throw new ArgumentOutOfRangeException(nameof(options), "DefaultPageSize must be within MaximumPageSize.");
        if (_options.MaximumCursorBytes < CursorVersion + FingerprintLength + sizeof(int))
            throw new ArgumentOutOfRangeException(nameof(options), "MaximumCursorBytes is too small.");
        if (_options.MaximumSerializedValueBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), "MaximumSerializedValueBytes must be positive.");
        if (_options.MaximumIndexGenerations < 0 || indexes.Count > _options.MaximumIndexGenerations)
            throw new ArgumentOutOfRangeException(nameof(indexes), "The schema exceeds MaximumIndexGenerations.");
        if (_options.MaximumIndexEntriesPerEntity <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), "MaximumIndexEntriesPerEntity must be positive.");
        if (indexes.Any(static index => index is null))
            throw new ArgumentException("An index cannot be null.", nameof(indexes));
        var duplicate = indexes.GroupBy(static index => (index.Name, index.Generation))
            .FirstOrDefault(static group => group.Count() > 1);
        if (duplicate is not null)
            throw new ArgumentException(
                $"Index '{duplicate.Key.Name}' generation {duplicate.Key.Generation} is duplicated.", nameof(indexes));
        _name = name;
        _valueTypeInfo = valueTypeInfo;
        _identity = identity;
        _identityKey = identityKey;
        _indexes = [.. indexes];
    }

    /// <summary>Inserts a new primary record and every configured index generation without reading first.</summary>
    public async ValueTask InsertAsync(IKvTransaction transaction, T value, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(value);
        var bytes = Serialize(value);
        var indexes = IndexKeys(value);
        await transaction.InsertAsync(PrimaryKey(_identity(value)), bytes, ct).ConfigureAwait(false);
        await PutIndexesAsync(transaction, indexes, bytes, ct).ConfigureAwait(false);
    }

    /// <summary>Replaces a known previous value without reading, maintaining every configured index.</summary>
    public async ValueTask ReplaceAsync(
        IKvTransaction transaction,
        T previous,
        T current,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(previous);
        ArgumentNullException.ThrowIfNull(current);
        var previousIdentity = _identity(previous);
        if (!EqualityComparer<TKey>.Default.Equals(previousIdentity, _identity(current)))
            throw new ArgumentException("A replacement cannot change the entity identity.", nameof(current));
        var bytes = Serialize(current);
        var previousIndexes = IndexKeys(previous);
        var currentIndexes = IndexKeys(current);
        await DeleteIndexesAsync(transaction, previousIndexes, ct).ConfigureAwait(false);
        await transaction.PutAsync(PrimaryKey(previousIdentity), bytes, ct).ConfigureAwait(false);
        await PutIndexesAsync(transaction, currentIndexes, bytes, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Conveniently inserts or replaces a value. This performs one primary-record read; use
    /// <see cref="InsertAsync"/> or <see cref="ReplaceAsync"/> when the caller knows the prior state.
    /// </summary>
    public async ValueTask UpsertAsync(IKvTransaction transaction, T value, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(value);
        var key = PrimaryKey(_identity(value));
        var existing = await transaction.GetAsync(key, ct).ConfigureAwait(false);
        if (existing.Found)
            await ReplaceAsync(transaction, Deserialize(existing.Value!.Value), value, ct).ConfigureAwait(false);
        else
            await InsertAsync(transaction, value, ct).ConfigureAwait(false);
    }

    /// <summary>Deletes a known value and all its configured index rows without reading first.</summary>
    public async ValueTask DeleteAsync(IKvTransaction transaction, T value, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(value);
        var indexes = IndexKeys(value);
        await DeleteIndexesAsync(transaction, indexes, ct).ConfigureAwait(false);
        await transaction.DeleteAsync(PrimaryKey(_identity(value)), ct).ConfigureAwait(false);
    }

    /// <summary>Conveniently deletes by identity, performing one read to discover old index keys.</summary>
    public async ValueTask DeleteAsync(IKvTransaction transaction, TKey identity, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var existing = await transaction.GetAsync(PrimaryKey(identity), ct).ConfigureAwait(false);
        if (existing.Found)
            await DeleteAsync(transaction, Deserialize(existing.Value!.Value), ct).ConfigureAwait(false);
    }

    /// <summary>Reads one primary record through a caller-owned transaction.</summary>
    public async ValueTask<T?> GetAsync(IKvTransaction transaction, TKey identity, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var result = await transaction.GetAsync(PrimaryKey(identity), ct).ConfigureAwait(false);
        return result.Found ? Deserialize(result.Value!.Value) : default;
    }

    /// <summary>Reads one primary record in a short read-only transaction.</summary>
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task",
        Justification = "The await-using declaration must retain the transaction type.")]
    public async ValueTask<T?> GetAsync(
        IKvClient client,
        string route,
        TKey identity,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        await using var transaction = await client.BeginAsync(route, KvDurability.Async, KvMode.ReadOnly, ct)
            .ConfigureAwait(false);
        return await GetAsync(transaction, identity, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes one bounded keyset query against a configured covering index through a caller-owned
    /// transaction. A read-write transaction sees its own staged writes. Cursors are bound to
    /// <see cref="IKvTransaction.Route"/>, so a cursor issued for one route is rejected on another.
    /// </summary>
    /// <exception cref="NotSupportedException">The transaction does not report its route.</exception>
    public async ValueTask<Page<T>> QueryAsync(
        IKvTransaction transaction,
        KvDirectoryQuery<T> query,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(query);
        var plan = Plan(transaction.Route, query);
        return await ExecuteAsync(transaction, plan, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes one bounded keyset query against a configured covering index in a short read-only
    /// transaction. The query and cursor are validated before the transaction is opened.
    /// </summary>
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task",
        Justification = "The await-using declaration must retain the transaction type.")]
    public async ValueTask<Page<T>> QueryAsync(
        IKvClient client,
        string route,
        KvDirectoryQuery<T> query,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrWhiteSpace(route);
        ArgumentNullException.ThrowIfNull(query);
        var plan = Plan(route, query);
        await using var transaction = await client.BeginAsync(route, KvDurability.Async, KvMode.ReadOnly, ct)
            .ConfigureAwait(false);
        return await ExecuteAsync(transaction, plan, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Backfills one configured index generation from stable primary records in a committed,
    /// resumable batch. Deploy the upgraded schema first so ordinary writes dual-write old and new generations.
    /// </summary>
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task",
        Justification = "The await-using declaration must retain the transaction type.")]
    public async ValueTask<KvDirectoryBackfillPage> BackfillAsync(
        IKvClient client,
        string route,
        KvDirectoryIndex<T> index,
        int limit,
        string? cursor = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrWhiteSpace(route);
        ArgumentNullException.ThrowIfNull(index);
        index = Resolve(index);
        ValidateLimit(limit);
        var primaryPrefix = PrimaryPrefix();
        var rangeStart = LexKey.EncodeFirst(primaryPrefix).AsMemory();
        var rangeEnd = LexKey.EncodeLast(primaryPrefix).AsMemory();
        var fingerprint = Fingerprint(route, index, false, [], "backfill");
        var cursorKey = DecodeCursor(cursor, fingerprint);
        ValidateCursorRange(cursorKey, rangeStart, rangeEnd);
        var scan = new KvScanQuery(
            cursorKey is null ? rangeStart : After(cursorKey.Value.Span), rangeEnd, (uint)(limit + 1));
        await using var transaction = await client.BeginAsync(route, KvDurability.Async, KvMode.ReadWrite, ct)
            .ConfigureAwait(false);
        var records = new List<KvPair>(limit + 1);
        await foreach (var pair in transaction.ScanAllAsync(scan, ct).ConfigureAwait(false))
        {
            records.Add(pair);
            if (records.Count > limit)
                break;
        }
        var processed = records.Take(limit).ToArray();
        foreach (var record in processed)
        {
            var value = Deserialize(record.Value);
            foreach (var key in IndexKeys(index, value))
                await transaction.PutAsync(key, record.Value, ct).ConfigureAwait(false);
        }
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        var next = records.Count > limit ? EncodeCursor(fingerprint, processed[^1].Key) : null;
        return new KvDirectoryBackfillPage(processed.Length, next);
    }

    /// <summary>
    /// Deletes every row for an obsolete index generation in one transactional range mutation.
    /// Call only after readers and writers have cut over to a newer generation.
    /// </summary>
    public ValueTask DeleteIndexGenerationAsync(
        IKvTransaction transaction,
        KvDirectoryIndex<T> index,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(index);
        var prefix = IndexPrefix(index, []);
        return new ValueTask(transaction.DeleteRangeAsync(
            LexKey.EncodeFirst(prefix).AsMemory(), LexKey.EncodeLast(prefix).AsMemory(), ct));
    }

    KvDirectoryIndex<T> Resolve(KvDirectoryIndex<T> requested) =>
        _indexes.FirstOrDefault(index =>
            string.Equals(index.Name, requested.Name, StringComparison.Ordinal) &&
            index.Generation == requested.Generation)
        ?? throw new KvDirectoryQueryException(KvDirectoryQueryError.UnsupportedIndex,
            $"Index '{requested.Name}' generation {requested.Generation} is not configured.");

    void ValidateLimit(int limit)
    {
        if (limit <= 0 || limit > _options.MaximumPageSize)
            throw new KvDirectoryQueryException(KvDirectoryQueryError.InvalidLimit,
                $"The limit must be between 1 and {_options.MaximumPageSize}.");
    }

    static async ValueTask PutIndexesAsync(
        IKvTransaction transaction,
        IReadOnlyList<ReadOnlyMemory<byte>[]> indexes,
        ReadOnlyMemory<byte> bytes,
        CancellationToken ct)
    {
        foreach (var index in indexes)
        {
            foreach (var key in index)
                await transaction.PutAsync(key, bytes, ct).ConfigureAwait(false);
        }
    }

    static async ValueTask DeleteIndexesAsync(
        IKvTransaction transaction,
        IReadOnlyList<ReadOnlyMemory<byte>[]> indexes,
        CancellationToken ct)
    {
        foreach (var index in indexes)
        {
            foreach (var key in index)
                await transaction.DeleteAsync(key, ct).ConfigureAwait(false);
        }
    }

    ReadOnlyMemory<byte> PrimaryKey(TKey identity) =>
        LexKey.EncodeComposite(Combine(PrimaryPrefix(), IdentityParts(identity))).AsMemory();

    LexKeyPart[] PrimaryPrefix() => [_name, "record"];

    ReadOnlyMemory<byte>[][] IndexKeys(T value)
    {
        var result = new ReadOnlyMemory<byte>[_indexes.Length][];
        for (var position = 0; position < _indexes.Length; position++)
            result[position] = IndexKeys(_indexes[position], value);
        return result;
    }

    ReadOnlyMemory<byte>[] IndexKeys(KvDirectoryIndex<T> index, T value)
    {
        var selected = index.Select(value);
        if (selected.Count > _options.MaximumIndexEntriesPerEntity)
            throw new InvalidOperationException(
                $"Index '{index.Name}' exceeds MaximumIndexEntriesPerEntity.");
        var identity = IdentityParts(_identity(value));
        var result = new ReadOnlyMemory<byte>[selected.Count];
        for (var position = 0; position < selected.Count; position++)
        {
            var key = selected[position]
                ?? throw new InvalidOperationException($"Index '{index.Name}' returned a null key.");
            result[position] = LexKey.EncodeComposite(Combine(IndexPrefix(index, key), identity)).AsMemory();
        }
        return result;
    }

    LexKeyPart[] IndexPrefix(KvDirectoryIndex<T> index, LexKeyPart[] suffix) =>
        Combine([_name, "index", index.Name, index.Generation], suffix);

    LexKeyPart[] IdentityParts(TKey identity)
    {
        var parts = _identityKey(identity)
            ?? throw new InvalidOperationException("The identity selector returned a null key.");
        if (parts.Length == 0)
            throw new InvalidOperationException("The identity selector must return at least one part.");
        return parts;
    }

    static LexKeyPart[] Combine(LexKeyPart[] first, LexKeyPart[] second)
    {
        var result = new LexKeyPart[first.Length + second.Length];
        first.CopyTo(result, 0);
        second.CopyTo(result, first.Length);
        return result;
    }

    byte[] Serialize(T value)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(value, _valueTypeInfo);
        if (bytes.Length > _options.MaximumSerializedValueBytes)
            throw new InvalidOperationException(
                $"The serialized directory value exceeds {_options.MaximumSerializedValueBytes} bytes.");
        return bytes;
    }

    T Deserialize(ReadOnlyMemory<byte> value) =>
        JsonSerializer.Deserialize(value.Span, _valueTypeInfo)
        ?? throw new InvalidOperationException("The stored directory entry could not be deserialized.");

    QueryPlan Plan(string route, KvDirectoryQuery<T> query)
    {
        var index = Resolve(query.Index);
        var limit = query.Limit ?? _options.DefaultPageSize;
        ValidateLimit(limit);
        var prefix = IndexPrefix(index, query.Prefix);
        var rangeStart = LexKey.EncodeFirst(prefix).AsMemory();
        var rangeEnd = LexKey.EncodeLast(prefix).AsMemory();
        var fingerprint = Fingerprint(route, index, query.IsDescending, query.Prefix, "query");
        var cursorKey = DecodeCursor(query.Cursor, fingerprint);
        ValidateCursorRange(cursorKey, rangeStart, rangeEnd);
        var scan = query.IsDescending
            ? new KvScanQuery(rangeStart, cursorKey ?? rangeEnd, (uint)(limit + 1), Reverse: true)
            : new KvScanQuery(cursorKey is null ? rangeStart : After(cursorKey.Value.Span), rangeEnd,
                (uint)(limit + 1));
        return new QueryPlan(scan, fingerprint, limit);
    }

    async ValueTask<Page<T>> ExecuteAsync(IKvTransaction transaction, QueryPlan plan, CancellationToken ct)
    {
        var matches = new List<KvPair>(plan.Limit + 1);
        await foreach (var pair in transaction.ScanAllAsync(plan.Scan, ct).ConfigureAwait(false))
        {
            matches.Add(pair);
            if (matches.Count > plan.Limit)
                break;
        }
        var hasMore = matches.Count > plan.Limit;
        var returned = matches.Take(plan.Limit).ToArray();
        var items = returned.Select(pair => Deserialize(pair.Value)).ToArray();
        var next = hasMore ? EncodeCursor(plan.Fingerprint, returned[^1].Key) : null;
        return new Page<T>(items, next);
    }

    readonly record struct QueryPlan(KvScanQuery Scan, byte[] Fingerprint, int Limit);

    byte[] Fingerprint(
        string route,
        KvDirectoryIndex<T> index,
        bool descending,
        LexKeyPart[] prefix,
        string purpose)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Append(hash, purpose);
        Append(hash, route);
        Append(hash, _name);
        Append(hash, index.Name);
        Span<byte> scalar = stackalloc byte[5];
        BinaryPrimitives.WriteUInt32BigEndian(scalar, index.Generation);
        scalar[4] = descending ? (byte)1 : (byte)0;
        hash.AppendData(scalar);
        hash.AppendData(LexKey.EncodeComposite(prefix).AsSpan());
        return hash.GetHashAndReset()[..FingerprintLength];
    }

    static void Append(IncrementalHash hash, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(length, bytes.Length);
        hash.AppendData(length);
        hash.AppendData(bytes);
    }

    string EncodeCursor(byte[] fingerprint, ReadOnlyMemory<byte> key)
    {
        var payload = new byte[CursorVersion + FingerprintLength + sizeof(int) + key.Length];
        payload[0] = CursorVersion;
        fingerprint.CopyTo(payload, 1);
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(1 + FingerprintLength), key.Length);
        key.Span.CopyTo(payload.AsSpan(1 + FingerprintLength + sizeof(int)));
        if (payload.Length > _options.MaximumCursorBytes)
            throw new InvalidOperationException("The generated cursor exceeds MaximumCursorBytes.");
        return Convert.ToBase64String(payload);
    }

    ReadOnlyMemory<byte>? DecodeCursor(string? cursor, byte[] expectedFingerprint)
    {
        if (cursor is null)
            return null;
        if (cursor.Length > _options.MaximumCursorBytes * 2L)
            throw InvalidCursor("The cursor exceeds the configured limit.");
        try
        {
            var payload = Convert.FromBase64String(cursor);
            if (payload.Length > _options.MaximumCursorBytes ||
                payload.Length < CursorVersion + FingerprintLength + sizeof(int) ||
                payload[0] != CursorVersion)
                throw InvalidCursor("The cursor has an invalid format or version.");
            var fingerprint = payload.AsSpan(1, FingerprintLength);
            if (!CryptographicOperations.FixedTimeEquals(fingerprint, expectedFingerprint))
                throw new KvDirectoryQueryException(KvDirectoryQueryError.CursorMismatch,
                    "The cursor belongs to a different directory query.");
            var keyLength = BinaryPrimitives.ReadInt32BigEndian(payload.AsSpan(1 + FingerprintLength));
            var keyOffset = 1 + FingerprintLength + sizeof(int);
            if (keyLength <= 0 || keyLength != payload.Length - keyOffset)
                throw InvalidCursor("The cursor key length is invalid.");
            return payload.AsMemory(keyOffset, keyLength);
        }
        catch (FormatException error)
        {
            throw InvalidCursor("The cursor is not valid Base64.", error);
        }
        catch (OverflowException error)
        {
            throw InvalidCursor("The cursor exceeds the configured limit.", error);
        }
    }

    static KvDirectoryQueryException InvalidCursor(string message, Exception? inner = null) =>
        new(KvDirectoryQueryError.InvalidCursor, message, inner);

    static void ValidateCursorRange(
        ReadOnlyMemory<byte>? cursorKey,
        ReadOnlyMemory<byte> rangeStart,
        ReadOnlyMemory<byte> rangeEnd)
    {
        if (cursorKey is not { } key)
            return;
        if (key.Span.SequenceCompareTo(rangeStart.Span) < 0 || key.Span.SequenceCompareTo(rangeEnd.Span) >= 0)
            throw InvalidCursor("The cursor key falls outside the selected index range.");
    }

    static byte[] After(ReadOnlySpan<byte> key)
    {
        var result = new byte[key.Length + 1];
        key.CopyTo(result);
        return result;
    }
}
