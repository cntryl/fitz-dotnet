using System.Threading.Channels;

namespace Cntryl.Fitz.Testing;

/// <summary>
/// Provides a route-isolated, transactional <see cref="IKvClient"/> for consumer tests.
/// </summary>
/// <remarks>
/// Transactions read from a snapshot, see their own writes, and use optimistic route-level
/// conflict detection at commit. All input and output bytes are cloned. The type is safe for
/// concurrent test code and has no dependency on a test or assertion framework.
/// </remarks>
public sealed class InMemoryKvClient : IKvClient
{
    readonly object _gate = new();
    readonly Dictionary<string, RouteState> _routes = new(StringComparer.Ordinal);
    readonly List<OperationEntry> _operations = [];
    readonly List<FaultEntry> _faults = [];
    readonly Dictionary<long, SubscriptionEntry> _subscriptions = [];
    readonly int? _scanPageSize;
    readonly int? _subscriptionBufferCapacity;
    long _nextSubscriptionId;
    long _nextTransactionId;
    long _generation;

    /// <summary>Initializes an in-memory client with default behavior.</summary>
    public InMemoryKvClient()
        : this(new InMemoryKvClientOptions())
    {
    }

    /// <summary>Initializes an in-memory client with explicit behavior.</summary>
    /// <param name="options">Testing behavior.</param>
    public InMemoryKvClient(InMemoryKvClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.ScanPageSize is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "ScanPageSize must be positive when supplied.");
        }
        if (options.SubscriptionBufferCapacity is <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "SubscriptionBufferCapacity must be positive when supplied.");
        }

        _scanPageSize = options.ScanPageSize;
        _subscriptionBufferCapacity = options.SubscriptionBufferCapacity;
    }

    /// <summary>Gets cloned records of operations observed so far.</summary>
    public IReadOnlyList<KvTestOperationRecord> Operations
    {
        get
        {
            lock (_gate)
            {
                return _operations.Select(static operation => operation.ToPublic()).ToArray();
            }
        }
    }

    /// <summary>Seeds or replaces a committed value without opening a transaction.</summary>
    /// <param name="route">Exact KV route.</param>
    /// <param name="key">Key bytes.</param>
    /// <param name="value">Value bytes.</param>
    public void Seed(string route, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        ValidateRoute(route);
        lock (_gate)
        {
            var state = GetOrCreateRoute(route);
            state.Values[key.ToArray()] = value.ToArray();
            state.Version++;
        }
    }

    /// <summary>Seeds or replaces several committed values without opening a transaction.</summary>
    /// <param name="route">Exact KV route.</param>
    /// <param name="pairs">Pairs to clone into the route.</param>
    public void Seed(string route, params KvPair[] pairs)
    {
        ArgumentNullException.ThrowIfNull(pairs);
        ValidateRoute(route);
        lock (_gate)
        {
            var state = GetOrCreateRoute(route);
            foreach (var pair in pairs)
            {
                ArgumentNullException.ThrowIfNull(pair);
                state.Values[pair.Key.ToArray()] = pair.Value.ToArray();
            }
            state.Version++;
        }
    }

    /// <summary>Reads one deeply cloned committed value without opening a transaction.</summary>
    /// <param name="route">Exact KV route.</param>
    /// <param name="key">Key bytes.</param>
    /// <returns>The committed value, when found.</returns>
    public KvGetResult Read(string route, ReadOnlyMemory<byte> key)
    {
        ValidateRoute(route);
        lock (_gate)
        {
            return _routes.TryGetValue(route, out var state) &&
                state.Values.TryGetValue(key.ToArray(), out var value)
                ? new KvGetResult(true, value.ToArray())
                : new KvGetResult(false);
        }
    }

    /// <summary>Returns a sorted, deeply cloned snapshot of committed route contents.</summary>
    /// <param name="route">Exact KV route.</param>
    public IReadOnlyList<KvPair> Snapshot(string route)
    {
        ValidateRoute(route);
        lock (_gate)
        {
            return _routes.TryGetValue(route, out var state)
                ? ClonePairs(state.Values)
                : [];
        }
    }

    /// <summary>Removes committed values for one route.</summary>
    /// <param name="route">Exact KV route.</param>
    public void Clear(string route)
    {
        ValidateRoute(route);
        lock (_gate)
        {
            if (_routes.TryGetValue(route, out var state))
            {
                state.Values.Clear();
                state.Version++;
            }
        }
    }

    /// <summary>Clears operation history while preserving committed data and queued faults.</summary>
    public void ClearOperations()
    {
        lock (_gate)
        {
            _operations.Clear();
        }
    }

    /// <summary>Clears queued fault injections while preserving committed data and history.</summary>
    public void ClearFaults()
    {
        lock (_gate)
        {
            _faults.Clear();
        }
    }

    /// <summary>Clears committed data, history, faults, and active subscriptions.</summary>
    public void Reset()
    {
        List<Channel<KvNotification>> subscriptions;
        lock (_gate)
        {
            _routes.Clear();
            _operations.Clear();
            _faults.Clear();
            _generation++;
            _nextSubscriptionId = 0;
            _nextTransactionId = 0;
            subscriptions = _subscriptions.Values.Select(static entry => entry.Channel).ToList();
            _subscriptions.Clear();
        }
        foreach (var subscription in subscriptions)
        {
            subscription.Writer.TryComplete();
        }
    }

    /// <summary>Queues an exception for the next matching operation.</summary>
    /// <param name="operation">Operation to fail.</param>
    /// <param name="exception">Exception thrown from the operation.</param>
    /// <param name="route">Optional exact route filter.</param>
    public void FailNext(KvTestOperation operation, Exception exception, string? route = null)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (route is not null)
        {
            ValidateRoute(route);
        }

        lock (_gate)
        {
            _faults.Add(new FaultEntry(operation, route, exception));
        }
    }

    /// <summary>Queues a structured KV failure for the next matching operation.</summary>
    /// <param name="operation">Operation to fail.</param>
    /// <param name="code">Symbolic error code exposed by <see cref="KvException.Code"/>.</param>
    /// <param name="message">Optional diagnostic message.</param>
    /// <param name="route">Optional exact route filter.</param>
    public void FailNext(
        KvTestOperation operation,
        string code,
        string? message = null,
        string? route = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(code);
        FailNext(operation, new KvException(message ?? $"Injected {operation} failure.", code), route);
    }

    /// <inheritdoc />
    public Task<IKvTransaction> BeginAsync(
        string route,
        KvDurability durability,
        KvMode mode = KvMode.ReadWrite,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        ValidateRoute(route);
        if (durability is not KvDurability.Async and not KvDurability.Sync)
        {
            throw new ArgumentOutOfRangeException(nameof(durability));
        }
        if (mode is not KvMode.ReadOnly and not KvMode.ReadWrite)
        {
            throw new ArgumentOutOfRangeException(nameof(mode));
        }

        lock (_gate)
        {
            var transactionId = ++_nextTransactionId;
            RecordAndFault(transactionId, KvTestOperation.Begin, route, mode, durability: durability);
            var state = GetOrCreateRoute(route);
            IKvTransaction transaction = new InMemoryKvTransaction(
                this,
                transactionId,
                route,
                mode,
                _generation,
                state.Version,
                CloneValues(state.Values),
                _scanPageSize);
            return Task.FromResult(transaction);
        }
    }

    /// <inheritdoc />
    public Task<KvSubscription> SubscribeAsync(string pattern, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        ValidatePattern(pattern);
        lock (_gate)
        {
            RecordAndFault(null, KvTestOperation.Subscribe, pattern);
            var id = ++_nextSubscriptionId;
            var channel = CreateSubscriptionChannel();
            _subscriptions[id] = new SubscriptionEntry(pattern, channel);
            var subscription = new KvSubscription(
                pattern,
                channel.Reader.ReadAllAsync(CancellationToken.None),
                _ => RemoveSubscriptionAsync(id));
            return Task.FromResult(subscription);
        }
    }

    Channel<KvNotification> CreateSubscriptionChannel() => _subscriptionBufferCapacity is { } capacity
        ? Channel.CreateBounded<KvNotification>(new BoundedChannelOptions(capacity)
        {
            SingleReader = false,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false,
        })
        : Channel.CreateUnbounded<KvNotification>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });

    internal void RecordAndFault(
        long? transactionId,
        KvTestOperation operation,
        string route,
        KvMode? mode = null,
        ReadOnlyMemory<byte>? key = null,
        ReadOnlyMemory<byte>? value = null,
        ReadOnlyMemory<byte>? endKey = null,
        KvScanQuery? scanQuery = null,
        KvDurability? durability = null)
    {
        lock (_gate)
        {
            _operations.Add(new OperationEntry(
                transactionId,
                operation,
                route,
                durability,
                mode,
                key?.ToArray(),
                value?.ToArray(),
                endKey?.ToArray(),
                CloneQuery(scanQuery)));
            var index = _faults.FindIndex(fault =>
                fault.Operation == operation &&
                (fault.Route is null || string.Equals(fault.Route, route, StringComparison.Ordinal)));
            if (index >= 0)
            {
                var exception = _faults[index].Exception;
                _faults.RemoveAt(index);
                throw exception;
            }
        }
    }

    internal void Commit(
        string route,
        long generation,
        long snapshotVersion,
        SortedDictionary<byte[], byte[]> values,
        ulong mutationCount)
    {
        List<Channel<KvNotification>> subscribers;
        lock (_gate)
        {
            EnsureGeneration(generation);
            var state = GetOrCreateRoute(route);
            if (mutationCount > 0 && state.Version != snapshotVersion)
            {
                throw new KvException(
                    "The transaction lost an isolation conflict.",
                    "ISOLATION_CONFLICT",
                    domainCode: FitzErrorCodes.KvIsolationConflict);
            }

            if (mutationCount == 0)
            {
                return;
            }

            state.Values = CloneValues(values);
            state.Version++;
            subscribers = _subscriptions.Values
                .Where(subscription => PatternMatches(subscription.Pattern, route))
                .Select(static subscription => subscription.Channel)
                .ToList();
        }

        var notification = new KvNotification(route, mutationCount);
        foreach (var subscriber in subscribers)
        {
            if (!subscriber.Writer.TryWrite(notification))
            {
                subscriber.Writer.TryComplete(new SubscriptionBackpressureException(
                    "The in-memory KV subscription buffer overflowed."));
            }
        }
    }

    static SortedDictionary<byte[], byte[]> CloneValues(SortedDictionary<byte[], byte[]> source)
    {
        var clone = new SortedDictionary<byte[], byte[]>(ByteArrayComparer.Instance);
        foreach (var pair in source)
        {
            clone[pair.Key.ToArray()] = pair.Value.ToArray();
        }
        return clone;
    }

    static KvPair[] ClonePairs(SortedDictionary<byte[], byte[]> source) =>
        source.Select(static pair => new KvPair(pair.Key.ToArray(), pair.Value.ToArray())).ToArray();

    static KvScanQuery? CloneQuery(KvScanQuery? query) => query is null
        ? null
        : query with
        {
            StartKey = CloneMemory(query.StartKey),
            EndKey = CloneMemory(query.EndKey),
        };

    static ReadOnlyMemory<byte>? CloneMemory(ReadOnlyMemory<byte>? value) =>
        value.HasValue ? new ReadOnlyMemory<byte>(value.Value.ToArray()) : null;

    RouteState GetOrCreateRoute(string route)
    {
        if (!_routes.TryGetValue(route, out var state))
        {
            state = new RouteState();
            _routes.Add(route, state);
        }
        return state;
    }

    internal void EnsureGeneration(long generation)
    {
        lock (_gate)
        {
            if (generation != _generation)
            {
                throw new KvException("The transaction was invalidated when the test client was reset.", "TX_CLOSED");
            }
        }
    }

    ValueTask RemoveSubscriptionAsync(long id)
    {
        lock (_gate)
        {
            if (_subscriptions.Remove(id, out var entry))
            {
                entry.Channel.Writer.TryComplete();
            }
        }
        return ValueTask.CompletedTask;
    }

    static void ValidateRoute(string route)
    {
        if (!TrySegments(route, out var segments) ||
            segments.Length != 3 ||
            segments.Any(static segment => segment.Contains('*', StringComparison.Ordinal)))
        {
            throw new KvException($"route '{route}' must be kv://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }
    }

    static void ValidatePattern(string pattern)
    {
        if (!TrySegments(pattern, out var segments) ||
            segments.Any(static segment =>
                segment.Contains('*', StringComparison.Ordinal) && segment is not "*" and not "**") ||
            segments.Zip(segments.Skip(1), static (left, right) => left == "**" && right == "**").Any(static adjacent => adjacent) ||
            (segments.Length - segments.Count(static segment => segment == "**") > 3) ||
            (segments.All(static segment => segment != "**") && segments.Length != 3))
        {
            throw new KvException(
                $"pattern '{pattern}' must use whole-segment wildcards and match a three-segment KV route",
                "INVALID_ROUTE");
        }
    }

    static bool TrySegments(string route, out string[] segments)
    {
        segments = [];
        const string prefix = "kv://";
        if (!route.StartsWith(prefix, StringComparison.Ordinal) ||
            route.Length == prefix.Length ||
            route.Contains('?', StringComparison.Ordinal) ||
            route.Contains('#', StringComparison.Ordinal))
        {
            return false;
        }

        segments = route[prefix.Length..].Split('/');
        return segments.All(static segment => segment.Length > 0);
    }

    static bool PatternMatches(string pattern, string route)
    {
        _ = TrySegments(pattern, out var patternSegments);
        _ = TrySegments(route, out var routeSegments);
        var routeIndex = 0;
        var patternIndex = 0;
        var lastDoubleWildcard = -1;
        var lastDoubleMatch = 0;
        while (routeIndex < routeSegments.Length)
        {
            var segment = patternIndex < patternSegments.Length ? patternSegments[patternIndex] : null;
            if (segment == "*" || string.Equals(segment, routeSegments[routeIndex], StringComparison.Ordinal))
            {
                routeIndex++;
                patternIndex++;
                continue;
            }
            if (segment == "**")
            {
                lastDoubleWildcard = patternIndex++;
                lastDoubleMatch = routeIndex;
                continue;
            }
            if (lastDoubleWildcard < 0)
            {
                return false;
            }
            routeIndex = ++lastDoubleMatch;
            patternIndex = lastDoubleWildcard + 1;
        }
        while (patternIndex < patternSegments.Length && patternSegments[patternIndex] == "**")
        {
            patternIndex++;
        }
        return patternIndex == patternSegments.Length;
    }

    sealed class RouteState
    {
        internal SortedDictionary<byte[], byte[]> Values { get; set; } = new(ByteArrayComparer.Instance);
        internal long Version { get; set; }
    }

    sealed record OperationEntry(
        long? TransactionId,
        KvTestOperation Operation,
        string Route,
        KvDurability? Durability,
        KvMode? Mode,
        byte[]? Key,
        byte[]? Value,
        byte[]? EndKey,
        KvScanQuery? ScanQuery)
    {
        internal KvTestOperationRecord ToPublic() => new(
            TransactionId,
            Operation,
            Route,
            Durability,
            Mode,
            Key?.ToArray(),
            Value?.ToArray(),
            EndKey?.ToArray(),
            CloneQuery(ScanQuery));
    }

    sealed record FaultEntry(KvTestOperation Operation, string? Route, Exception Exception);
    sealed record SubscriptionEntry(string Pattern, Channel<KvNotification> Channel);

    internal sealed class ByteArrayComparer : IComparer<byte[]>, IEqualityComparer<byte[]>
    {
        internal static ByteArrayComparer Instance { get; } = new();

        public int Compare(byte[]? left, byte[]? right)
        {
            if (ReferenceEquals(left, right))
            {
                return 0;
            }
            if (left is null)
            {
                return -1;
            }
            if (right is null)
            {
                return 1;
            }
            return left.AsSpan().SequenceCompareTo(right);
        }

        public bool Equals(byte[]? left, byte[]? right) => Compare(left, right) == 0;
        public int GetHashCode(byte[] value)
        {
            var hash = new HashCode();
            hash.AddBytes(value);
            return hash.ToHashCode();
        }
    }

    sealed class InMemoryKvTransaction : IKvTransaction
    {
        readonly InMemoryKvClient _owner;
        readonly long _transactionId;
        readonly string _route;
        readonly KvMode _mode;
        readonly long _generation;
        readonly long _snapshotVersion;
        readonly int? _scanPageSize;
        readonly object _gate = new();
        readonly SortedDictionary<byte[], byte[]> _values;
        ulong _mutationCount;
        bool _closed;

        internal InMemoryKvTransaction(
            InMemoryKvClient owner,
            long transactionId,
            string route,
            KvMode mode,
            long generation,
            long snapshotVersion,
            SortedDictionary<byte[], byte[]> values,
            int? scanPageSize)
        {
            _owner = owner;
            _transactionId = transactionId;
            _route = route;
            _mode = mode;
            _generation = generation;
            _snapshotVersion = snapshotVersion;
            _values = values;
            _scanPageSize = scanPageSize;
        }

        public string Route => _route;

        public Task<KvGetResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            lock (_gate)
            {
                EnsureOpen();
                _owner.RecordAndFault(_transactionId, KvTestOperation.Get, _route, _mode, key);
                return Task.FromResult(_values.TryGetValue(key.ToArray(), out var value)
                    ? new KvGetResult(true, value.ToArray())
                    : new KvGetResult(false));
            }
        }

        public Task PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default) =>
            WriteAsync(KvTestOperation.Put, key, value, insertOnly: false, ct);

        public Task InsertAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default) =>
            WriteAsync(KvTestOperation.Insert, key, value, insertOnly: true, ct);

        public Task DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            lock (_gate)
            {
                EnsureWritable();
                _owner.RecordAndFault(_transactionId, KvTestOperation.Delete, _route, _mode, key);
                _values.Remove(key.ToArray());
                _mutationCount++;
                return Task.CompletedTask;
            }
        }

        public Task DeleteRangeAsync(
            ReadOnlyMemory<byte> startKey,
            ReadOnlyMemory<byte> endKey,
            CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            lock (_gate)
            {
                EnsureWritable();
                _owner.RecordAndFault(
                    _transactionId,
                    KvTestOperation.DeleteRange,
                    _route,
                    _mode,
                    startKey,
                    endKey: endKey);
                var keys = _values.Keys.Where(key =>
                    ByteArrayComparer.Instance.Compare(key, startKey.ToArray()) >= 0 &&
                    ByteArrayComparer.Instance.Compare(key, endKey.ToArray()) < 0).ToArray();
                foreach (var key in keys)
                {
                    _values.Remove(key);
                }
                _mutationCount++;
                return Task.CompletedTask;
            }
        }

        public Task<KvScanResult> ScanAsync(KvScanQuery query, CancellationToken ct = default)
        {
            ArgumentNullException.ThrowIfNull(query);
            ct.ThrowIfCancellationRequested();
            lock (_gate)
            {
                EnsureOpen();
                _owner.RecordAndFault(
                    _transactionId,
                    KvTestOperation.Scan,
                    _route,
                    _mode,
                    query.StartKey,
                    scanQuery: query);
                IEnumerable<KeyValuePair<byte[], byte[]>> candidates = _values;
                if (query.StartKey is { } start)
                {
                    var startBytes = start.ToArray();
                    candidates = candidates.Where(pair => ByteArrayComparer.Instance.Compare(pair.Key, startBytes) >= 0);
                }
                if (query.EndKey is { } end)
                {
                    var endBytes = end.ToArray();
                    candidates = candidates.Where(pair => ByteArrayComparer.Instance.Compare(pair.Key, endBytes) < 0);
                }
                if (query.Reverse)
                {
                    candidates = candidates.Reverse();
                }

                var requested = query.Limit is null or 0 or > int.MaxValue
                    ? int.MaxValue
                    : (int)query.Limit.Value;
                var limit = _scanPageSize.HasValue ? Math.Min(requested, _scanPageSize.Value) : requested;
                var materialized = candidates.Take(limit == int.MaxValue ? int.MaxValue : limit + 1).ToArray();
                var hasMore = materialized.Length > limit;
                var pairs = materialized.Take(limit)
                    .Select(static pair => new KvPair(pair.Key.ToArray(), pair.Value.ToArray()))
                    .ToArray();
                return Task.FromResult(new KvScanResult(pairs, hasMore));
            }
        }

        public Task CommitAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            lock (_gate)
            {
                EnsureOpen();
                _owner.RecordAndFault(_transactionId, KvTestOperation.Commit, _route, _mode);
                _owner.Commit(_route, _generation, _snapshotVersion, _values, _mutationCount);
                _closed = true;
                return Task.CompletedTask;
            }
        }

        public Task RollbackAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            lock (_gate)
            {
                if (_closed)
                {
                    return Task.CompletedTask;
                }
                EnsureOpen();
                _owner.RecordAndFault(_transactionId, KvTestOperation.Rollback, _route, _mode);
                _closed = true;
                return Task.CompletedTask;
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Design",
            "CA1031:Do not catch general exception types",
            Justification = "Test-double disposal mirrors production best-effort rollback and must not hide the test's primary exception.")]
        public ValueTask DisposeAsync()
        {
            lock (_gate)
            {
                if (!_closed)
                {
                    try
                    {
                        _owner.EnsureGeneration(_generation);
                        _owner.RecordAndFault(_transactionId, KvTestOperation.Rollback, _route, _mode);
                    }
                    catch (Exception)
                    {
                        // Disposal mirrors the production client's best-effort rollback cleanup.
                    }
                }
                _closed = true;
            }
            return ValueTask.CompletedTask;
        }

        Task WriteAsync(
            KvTestOperation operation,
            ReadOnlyMemory<byte> key,
            ReadOnlyMemory<byte> value,
            bool insertOnly,
            CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            lock (_gate)
            {
                EnsureWritable();
                _owner.RecordAndFault(_transactionId, operation, _route, _mode, key, value);
                var keyBytes = key.ToArray();
                if (insertOnly && _values.ContainsKey(keyBytes))
                {
                    throw new KvException("The key already exists.", "KEY_EXISTS");
                }
                _values[keyBytes] = value.ToArray();
                _mutationCount++;
                return Task.CompletedTask;
            }
        }

        void EnsureWritable()
        {
            EnsureOpen();
            if (_mode == KvMode.ReadOnly)
            {
                throw new KvException("The transaction is read-only.", "READ_ONLY");
            }
        }

        void EnsureOpen()
        {
            if (_closed)
            {
                throw new KvException("The transaction is closed.", "TX_CLOSED");
            }
            _owner.EnsureGeneration(_generation);
        }
    }
}
