using System.Text;
using System.Threading.Channels;

namespace Cntryl.Fitz.Testing;

/// <summary>
/// Provides a route-isolated, transactional <see cref="IKvClient"/> for consumer tests.
/// </summary>
/// <remarks>
/// <para>
/// All input and output bytes are cloned. The type is safe for concurrent test code and has no
/// dependency on a test or assertion framework. It models the broker's observable behavior, not
/// its storage engine, and deliberately fails the way the broker (and the real client) fail.
/// </para>
/// <para>
/// <b>Isolation.</b> A read-write transaction takes an exclusive write lock on its route when it
/// begins; a second read-write <c>BeginAsync</c> on a route whose lock is held fails immediately
/// with a <see cref="KvException"/> whose <see cref="KvException.DomainCode"/> is
/// <see cref="FitzErrorCodes.KvIsolationConflict"/>. The lock is released when the transaction
/// commits, rolls back, is disposed, or is lost (see <see cref="ExpireOpenTransactions"/> and
/// <see cref="Reset"/>). Read-only transactions never take or wait on the lock and read the
/// committed state as of their <c>BeginAsync</c>. Commit does not validate reads. A mutating
/// commit also fails with the same isolation conflict when <see cref="Seed(string, ReadOnlyMemory{byte}, ReadOnlyMemory{byte})"/>
/// or <see cref="Clear"/> changed the route after the transaction began (an approximation, at
/// route granularity, of the broker's storage write-conflict check).
/// </para>
/// <para>
/// <b>Commit failure.</b> The broker removes a transaction as soon as it accepts a COMMIT, so
/// after a commit fails (isolation conflict or an injected fault) nothing was applied, the write
/// lock is released, and the transaction is gone from the broker. The handle you hold is not
/// closed, exactly like the real client's: every further operation, a retried commit, and
/// <c>RollbackAsync</c> throw a <see cref="KvException"/> with domain code 1050
/// (transaction not found), and <c>DisposeAsync</c> swallows that. The only fault that leaves the
/// transaction usable is a <see cref="KvException"/> carrying <see cref="FitzErrorCodes.KvBusy"/>,
/// because the broker never accepted that request.
/// </para>
/// <para>
/// <b>Mutations.</b> Every successful put, insert, delete, and delete-range counts as one mutation
/// toward the commit notification, including a delete of a missing key and a delete-range over an
/// empty range, as on the broker. A delete-range whose start is not below its end is rejected.
/// </para>
/// <para>
/// <b>Errors.</b> Broker-side failures are <see cref="KvException"/>s shaped like the real
/// client's: <see cref="KvException.Code"/> is <c>{OPERATION}_FAILED</c> (for example
/// <c>INSERT_FAILED</c>) and <see cref="KvException.DomainCode"/> carries the broker code (1054
/// write in read-only, 1055 key exists, 1050 transaction not found, 1053 isolation conflict, 1002
/// for a rejected request). Client-side rejections keep the real client's codes
/// (<c>INVALID_ROUTE</c>, <c>TX_CLOSED</c>). A request whose encoded frame would exceed
/// <see cref="InMemoryKvClientOptions.MaxFrameSize"/> throws <see cref="ProtocolException"/> before
/// anything is recorded as sent. The broker has no separate key or value size limit and accepts an
/// empty key, so this type does not either.
/// </para>
/// <para>
/// <b>Scans.</b> Bounds are directional: forward scans return <c>start &lt;= key &lt; end</c>,
/// reverse scans return <c>end &lt; key &lt;= start</c>, and <see cref="KvScanQuery.StartExclusive"/>
/// makes the start bound strict. Inverted bounds return an empty page. A page never exceeds the
/// broker's 1024 items or 65,529 bytes of encoded pairs, whichever is reached first, so a consumer
/// that ignores <see cref="KvScanResult.HasMore"/> fails here as it would against the broker.
/// <see cref="IKvTransaction.SupportsExclusiveScan"/> is <see langword="true"/>.
/// </para>
/// <para>
/// <b>Not modeled:</b> authorization, realm validation, idle transaction expiry on a clock,
/// key-level write conflicts, DROP, and per-session lock ownership (one client is one broker).
/// </para>
/// </remarks>
public sealed class InMemoryKvClient : IKvClient
{
    // Broker constants. Sources: fitz src/domains/kv/actor/scan.rs (MAX_SCAN_ITEMS),
    // src/domains/kv/scan_wire_budget.rs (response payload ceiling and envelope overhead).
    const int BrokerMaxScanItems = 1024;
    const int BrokerScanItemOverheadBytes = 8;
    const int BrokerScanResponseCeilingBytes = ushort.MaxValue - 6;

    // Broker KV codes not exposed by FitzErrorCodes (docs/clients/spec/error-codes.md).
    const uint TransactionNotFoundCode = 1050;
    const uint WriteInReadOnlyCode = 1054;
    const uint KeyExistsCode = 1055;
    const uint InvalidRequestCode = 1002;

    readonly object _gate = new();
    readonly Dictionary<string, RouteState> _routes = [with(StringComparer.Ordinal)];
    readonly Dictionary<string, InMemoryKvTransaction> _writers = [with(StringComparer.Ordinal)];
    readonly HashSet<InMemoryKvTransaction> _openTransactions = [with(ReferenceEqualityComparer.Instance)];
    readonly List<OperationEntry> _operations = [];
    readonly List<FaultEntry> _faults = [];
    readonly Dictionary<long, SubscriptionEntry> _subscriptions = [];
    readonly int? _scanPageSize;
    readonly int? _subscriptionBufferCapacity;
    readonly int _maxFrameSize;
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
        if (options.MaxFrameSize is < FitzLimits.MinFrameSize or > FitzLimits.MaxFrameSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"MaxFrameSize must be between {FitzLimits.MinFrameSize} and {FitzLimits.MaxFrameSize} bytes.");
        }

        _scanPageSize = options.ScanPageSize;
        _subscriptionBufferCapacity = options.SubscriptionBufferCapacity;
        _maxFrameSize = options.MaxFrameSize;
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

    /// <summary>
    /// Seeds or replaces a committed value without opening a transaction. Subscribers whose
    /// pattern matches the route receive one notification with a mutation count of one, as for
    /// an out-of-band change on the broker.
    /// </summary>
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
            Notify(route, 1);
        }
    }

    /// <summary>
    /// Seeds or replaces several committed values without opening a transaction. Subscribers
    /// whose pattern matches the route receive one notification counting every pair written.
    /// </summary>
    /// <param name="route">Exact KV route.</param>
    /// <param name="pairs">Pairs to clone into the route.</param>
    public void Seed(string route, params KvPair[] pairs)
    {
        ArgumentNullException.ThrowIfNull(pairs);
        Seed(route, (IEnumerable<KvPair>)pairs);
    }

    /// <summary>
    /// Seeds or replaces several committed values without opening a transaction. Subscribers
    /// whose pattern matches the route receive one notification counting every pair written.
    /// </summary>
    /// <param name="route">Exact KV route.</param>
    /// <param name="pairs">Pairs to clone into the route.</param>
    public void Seed(string route, IEnumerable<KvPair> pairs)
    {
        ArgumentNullException.ThrowIfNull(pairs);
        ValidateRoute(route);
        var snapshot = pairs.ToArray();
        foreach (var pair in snapshot)
        {
            ArgumentNullException.ThrowIfNull(pair);
        }

        lock (_gate)
        {
            var state = GetOrCreateRoute(route);
            foreach (var pair in snapshot)
            {
                state.Values[pair.Key.ToArray()] = pair.Value.ToArray();
            }
            if (snapshot.Length > 0)
            {
                state.Version++;
                Notify(route, (ulong)snapshot.Length);
            }
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

    /// <summary>
    /// Removes committed values for one route. When the route held any, matching subscribers
    /// receive one notification counting the removed keys.
    /// </summary>
    /// <param name="route">Exact KV route.</param>
    public void Clear(string route)
    {
        ValidateRoute(route);
        lock (_gate)
        {
            if (_routes.TryGetValue(route, out var state) && state.Values.Count > 0)
            {
                var removed = (ulong)state.Values.Count;
                state.Values.Clear();
                state.Version++;
                Notify(route, removed);
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

    /// <summary>
    /// Clears committed data, history, faults, write locks, and active subscriptions, and
    /// invalidates every open transaction.
    /// </summary>
    public void Reset()
    {
        List<Channel<KvNotification>> subscriptions;
        lock (_gate)
        {
            _routes.Clear();
            _operations.Clear();
            _faults.Clear();
            _writers.Clear();
            _openTransactions.Clear();
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

    /// <summary>
    /// Simulates the broker discarding open transactions, as idle expiry or a disconnect does:
    /// each affected transaction is rolled back on the broker and releases its write lock, while
    /// the handles callers hold stay open and fail with "transaction not found" (domain code 1050)
    /// on their next use.
    /// </summary>
    /// <param name="route">Optional exact route filter; every route when omitted.</param>
    /// <returns>The number of transactions discarded.</returns>
    public int ExpireOpenTransactions(string? route = null)
    {
        if (route is not null)
        {
            ValidateRoute(route);
        }

        lock (_gate)
        {
            var expired = _openTransactions
                .Where(transaction => route is null || string.Equals(transaction.Route, route, StringComparison.Ordinal))
                .ToArray();
            foreach (var transaction in expired)
            {
                EndTransaction(transaction);
            }
            return expired.Length;
        }
    }

    /// <summary>
    /// Queues an exception for the next matching operation. The operation is recorded in
    /// <see cref="Operations"/> and then throws <paramref name="exception"/>, which counts as the
    /// broker or transport failing that request. A fault on <see cref="KvTestOperation.Rollback"/>
    /// leaves the transaction open on the broker, still holding its write lock; one on
    /// <see cref="KvTestOperation.Commit"/> consumes it (see the type remarks).
    /// </summary>
    /// <param name="operation">Operation to fail.</param>
    /// <param name="exception">Exception thrown from the operation.</param>
    /// <param name="route">Optional exact route filter.</param>
    /// <remarks>
    /// <c>DisposeAsync</c> performs the same best-effort rollback as the real client: it consumes
    /// and records a queued rollback fault but, like the real client, does not let a
    /// <see cref="FitzException"/>, <see cref="System.IO.IOException"/>, <see cref="OperationCanceledException"/>,
    /// or <see cref="TimeoutException"/> escape. A fault of any other type propagates from
    /// <c>DisposeAsync</c> so a test cannot mistake it for a swallowed failure. Call
    /// <c>RollbackAsync</c> explicitly to observe a rollback fault of any type.
    /// </remarks>
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
        KvMode mode = KvMode.ReadWrite,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        ValidateRoute(route);
        if (mode is not KvMode.ReadOnly and not KvMode.ReadWrite)
        {
            throw new ArgumentOutOfRangeException(nameof(mode));
        }
        ValidateFrame(sizeof(int) + Encoding.UTF8.GetByteCount(route) + sizeof(byte));

        lock (_gate)
        {
            var transactionId = ++_nextTransactionId;
            RecordAndFault(transactionId, KvTestOperation.Begin, route, mode);
            if (mode == KvMode.ReadWrite && _writers.ContainsKey(route))
            {
                throw BrokerError(
                    "BEGIN",
                    FitzErrorCodes.KvIsolationConflict,
                    "Transaction conflict: resource already has an active read-write transaction");
            }

            var state = GetOrCreateRoute(route);
            var transaction = new InMemoryKvTransaction(
                this,
                transactionId,
                route,
                mode,
                _generation,
                state.Version,
                CloneValues(state.Values),
                _scanPageSize);
            _openTransactions.Add(transaction);
            if (mode == KvMode.ReadWrite)
            {
                _writers[route] = transaction;
            }
            return Task.FromResult<IKvTransaction>(transaction);
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

    static KvException BrokerError(string operation, uint domainCode, string detail) =>
        new($"{operation} failed: {detail}", $"{operation}_FAILED", 1, domainCode);

    static string OperationName(KvTestOperation operation) => operation switch
    {
        KvTestOperation.Begin => "BEGIN",
        KvTestOperation.Get => "GET",
        KvTestOperation.Put => "PUT",
        KvTestOperation.Insert => "INSERT",
        KvTestOperation.Delete => "DELETE",
        KvTestOperation.DeleteRange => "DELETE_RANGE",
        KvTestOperation.Scan => "SCAN",
        KvTestOperation.Commit => "COMMIT",
        KvTestOperation.Rollback => "ROLLBACK",
        KvTestOperation.Subscribe => "SUBSCRIBE",
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };

    static KvException TransactionNotFound(KvTestOperation operation) =>
        BrokerError(OperationName(operation), TransactionNotFoundCode, "Invalid or unknown transaction ID");

    // The real client encodes the frame before sending, so an oversized request never reaches the
    // broker and is not recorded as an operation.
    internal void ValidateFrame(long payloadLength)
    {
        var encodedLength = FitzLimits.MinFrameSize + payloadLength;
        if (encodedLength > _maxFrameSize)
        {
            throw new ProtocolException(
                $"Encoded frame length {encodedLength} exceeds configured or negotiated limit {_maxFrameSize}.");
        }
    }

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
            if (RecordAndTakeFault(transactionId, operation, route, mode, key, value, endKey, scanQuery, durability)
                is { } fault)
            {
                throw fault;
            }
        }
    }

    // Records the operation and removes the first matching queued fault. The caller holds _gate.
    Exception? RecordAndTakeFault(
        long? transactionId,
        KvTestOperation operation,
        string route,
        KvMode? mode,
        ReadOnlyMemory<byte>? key,
        ReadOnlyMemory<byte>? value,
        ReadOnlyMemory<byte>? endKey,
        KvScanQuery? scanQuery,
        KvDurability? durability)
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
        if (index < 0)
        {
            return null;
        }

        var exception = _faults[index].Exception;
        _faults.RemoveAt(index);
        return exception;
    }

    // Common prologue of GET, PUT, INSERT, DELETE, DELETE_RANGE and SCAN: record, honor a queued
    // fault, then apply the broker's checks in its order (transaction lookup, then write mode).
    void Dispatch(
        InMemoryKvTransaction transaction,
        KvTestOperation operation,
        bool isWrite,
        ReadOnlyMemory<byte>? key = null,
        ReadOnlyMemory<byte>? value = null,
        ReadOnlyMemory<byte>? endKey = null,
        KvScanQuery? scanQuery = null)
    {
        lock (_gate)
        {
            EnsureGeneration(transaction.Generation);
            RecordAndFault(
                transaction.Id,
                operation,
                transaction.Route,
                transaction.Mode,
                key,
                value,
                endKey,
                scanQuery);
            if (transaction.BrokerEnded)
            {
                throw TransactionNotFound(operation);
            }
            if (isWrite && transaction.Mode == KvMode.ReadOnly)
            {
                throw BrokerError(OperationName(operation), WriteInReadOnlyCode, "Write attempted in a read-only transaction");
            }
        }
    }

    // Record, apply and publish under one lock so history order, commit order, and notification
    // order are the same total order.
    void Commit(
        InMemoryKvTransaction transaction,
        KvDurability durability,
        SortedDictionary<byte[], byte[]> values,
        ulong mutationCount)
    {
        lock (_gate)
        {
            EnsureGeneration(transaction.Generation);
            var fault = RecordAndTakeFault(
                transaction.Id,
                KvTestOperation.Commit,
                transaction.Route,
                transaction.Mode,
                null,
                null,
                null,
                null,
                durability);
            if (fault is not null)
            {
                // The broker removes the transaction once it accepts COMMIT; only a BUSY
                // response means the request was never accepted.
                if (fault is not KvException { DomainCode: FitzErrorCodes.KvBusy })
                {
                    EndTransaction(transaction);
                }
                throw fault;
            }
            if (transaction.BrokerEnded)
            {
                throw TransactionNotFound(KvTestOperation.Commit);
            }

            EndTransaction(transaction);
            if (mutationCount == 0)
            {
                return;
            }

            var state = GetOrCreateRoute(transaction.Route);
            if (state.Version != transaction.SnapshotVersion)
            {
                throw BrokerError(
                    "COMMIT",
                    FitzErrorCodes.KvIsolationConflict,
                    "Transaction conflict: the route changed after the transaction began");
            }

            state.Values = CloneValues(values);
            state.Version++;
            Notify(transaction.Route, mutationCount);
        }
    }

    void Rollback(InMemoryKvTransaction transaction)
    {
        lock (_gate)
        {
            EnsureGeneration(transaction.Generation);
            RecordAndFault(transaction.Id, KvTestOperation.Rollback, transaction.Route, transaction.Mode);
            if (transaction.BrokerEnded)
            {
                throw TransactionNotFound(KvTestOperation.Rollback);
            }
            EndTransaction(transaction);
        }
    }

    // Forgets a transaction on the broker side and releases its write lock. The caller holds _gate.
    void EndTransaction(InMemoryKvTransaction transaction)
    {
        transaction.MarkBrokerEnded();
        _openTransactions.Remove(transaction);
        if (_writers.TryGetValue(transaction.Route, out var writer) && ReferenceEquals(writer, transaction))
        {
            _writers.Remove(transaction.Route);
        }
    }

    // Delivers while holding _gate, so notifications leave in the order state changes happened.
    // Channel writes never block: a full bounded channel fails the subscription instead.
    void Notify(string route, ulong mutationCount)
    {
        if (mutationCount == 0)
        {
            return;
        }

        var notification = new KvNotification(route, mutationCount);
        foreach (var subscription in _subscriptions.Values)
        {
            if (!PatternMatches(subscription.Pattern, route))
            {
                continue;
            }
            if (!subscription.Channel.Writer.TryWrite(notification))
            {
                subscription.Channel.Writer.TryComplete(new SubscriptionBackpressureException(
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
            throw new ArgumentException($"route '{route}' must be kv://{{realm}}/{{area}}/{{resource}}", nameof(route));
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
            throw new ArgumentException($"pattern '{pattern}' must use whole-segment wildcards and match a three-segment KV route", nameof(pattern));
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
        internal SortedDictionary<byte[], byte[]> Values { get; set; } = [with(ByteArrayComparer.Instance)];
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
            Copy(Key),
            Copy(Value),
            Copy(EndKey),
            CloneQuery(ScanQuery));

        // A conditional with a null arm would pick ReadOnlyMemory<byte> through its implicit
        // array conversion and yield an empty value rather than null.
        static ReadOnlyMemory<byte>? Copy(byte[]? bytes) =>
            bytes is null ? default(ReadOnlyMemory<byte>?) : new ReadOnlyMemory<byte>(bytes.ToArray());
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
        readonly int? _scanPageSize;
        readonly object _gate = new();
        readonly SortedDictionary<byte[], byte[]> _values;
        readonly long _requestPrefixBytes;
        ulong _mutationCount;
        bool _closed;
        volatile bool _brokerEnded;

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
            Id = transactionId;
            Route = route;
            Mode = mode;
            Generation = generation;
            SnapshotVersion = snapshotVersion;
            _values = values;
            _scanPageSize = scanPageSize;
            // tx_id (u64) and the length-prefixed route open every KV request payload.
            _requestPrefixBytes = sizeof(ulong) + sizeof(int) + Encoding.UTF8.GetByteCount(route);
        }

        internal long Id { get; }

        public string Route { get; }

        internal KvMode Mode { get; }

        internal long Generation { get; }

        internal long SnapshotVersion { get; }

        // Set by the owner under its lock; read here under this transaction's lock.
        internal bool BrokerEnded => _brokerEnded;

        public bool SupportsExclusiveScan => true;

        internal void MarkBrokerEnded() => _brokerEnded = true;

        public Task<KvGetResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            lock (_gate)
            {
                EnsureOpen();
                _owner.ValidateFrame(_requestPrefixBytes + sizeof(int) + key.Length);
                _owner.Dispatch(this, KvTestOperation.Get, isWrite: false, key);
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
                EnsureOpen();
                _owner.ValidateFrame(_requestPrefixBytes + sizeof(int) + key.Length);
                _owner.Dispatch(this, KvTestOperation.Delete, isWrite: true, key);
                // The broker counts every accepted delete, whether or not the key exists.
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
                EnsureOpen();
                _owner.ValidateFrame(_requestPrefixBytes + sizeof(int) + startKey.Length + sizeof(int) + endKey.Length);
                _owner.Dispatch(this, KvTestOperation.DeleteRange, isWrite: true, startKey, endKey: endKey);
                var start = startKey.ToArray();
                var end = endKey.ToArray();
                if (ByteArrayComparer.Instance.Compare(start, end) >= 0)
                {
                    throw BrokerError(
                        "DELETE_RANGE",
                        InvalidRequestCode,
                        "Invalid request: start must be less than end");
                }

                var keys = _values.Keys.Where(key =>
                    ByteArrayComparer.Instance.Compare(key, start) >= 0 &&
                    ByteArrayComparer.Instance.Compare(key, end) < 0).ToArray();
                foreach (var key in keys)
                {
                    _values.Remove(key);
                }
                // An empty range still counts, as on the broker.
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
                _owner.ValidateFrame(ScanRequestBytes(query));
                _owner.Dispatch(this, KvTestOperation.Scan, isWrite: false, query.StartKey, scanQuery: query);
                return Task.FromResult(Scan(query));
            }
        }

        public Task CommitAsync(KvDurability durability, CancellationToken ct = default)
        {
            if (durability is not KvDurability.Buffered and not KvDurability.Sync)
            {
                throw new ArgumentOutOfRangeException(nameof(durability));
            }
            ct.ThrowIfCancellationRequested();
            lock (_gate)
            {
                EnsureOpen();
                _owner.ValidateFrame(_requestPrefixBytes + sizeof(byte));
                _owner.Commit(this, durability, _values, _mutationCount);
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
                _owner.ValidateFrame(_requestPrefixBytes);
                _owner.Rollback(this);
                _closed = true;
                return Task.CompletedTask;
            }
        }

        public ValueTask DisposeAsync()
        {
            lock (_gate)
            {
                try
                {
                    if (!_closed)
                    {
                        // Disposal is the real client's bounded best-effort rollback: failures a
                        // wire request can produce are swallowed, anything else is a test fault.
                        try
                        {
                            _owner.Rollback(this);
                        }
                        catch (FitzException)
                        {
                            // Broker rejection, such as "transaction not found" after a failed commit.
                        }
                        catch (System.IO.IOException)
                        {
                            // Transport failure.
                        }
                        catch (OperationCanceledException)
                        {
                            // Cleanup deadline.
                        }
                        catch (TimeoutException)
                        {
                            // Cleanup deadline.
                        }
                    }
                }
                finally
                {
                    _closed = true;
                }
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
                EnsureOpen();
                _owner.ValidateFrame(_requestPrefixBytes + sizeof(int) + key.Length + sizeof(int) + value.Length);
                _owner.Dispatch(this, operation, isWrite: true, key, value);
                var keyBytes = key.ToArray();
                if (insertOnly && _values.ContainsKey(keyBytes))
                {
                    throw BrokerError("INSERT", KeyExistsCode, "Key already exists");
                }
                _values[keyBytes] = value.ToArray();
                _mutationCount++;
                return Task.CompletedTask;
            }
        }

        // Mirrors the broker's SCAN: directional bounds, strict start on request, empty result for
        // inverted bounds, and a page capped by item count and by response bytes.
        KvScanResult Scan(KvScanQuery query)
        {
            var comparer = ByteArrayComparer.Instance;
            var start = query.StartKey?.ToArray();
            var end = query.EndKey?.ToArray();
            if (start is not null && end is not null &&
                (query.Reverse ? comparer.Compare(start, end) <= 0 : comparer.Compare(start, end) >= 0))
            {
                return new KvScanResult([], false);
            }

            IEnumerable<KeyValuePair<byte[], byte[]>> candidates = _values;
            if (query.Reverse)
            {
                if (start is not null)
                {
                    candidates = candidates.Where(pair => query.StartExclusive
                        ? comparer.Compare(pair.Key, start) < 0
                        : comparer.Compare(pair.Key, start) <= 0);
                }
                if (end is not null)
                {
                    candidates = candidates.Where(pair => comparer.Compare(pair.Key, end) > 0);
                }
                candidates = candidates.Reverse();
            }
            else
            {
                if (start is not null)
                {
                    candidates = candidates.Where(pair => query.StartExclusive
                        ? comparer.Compare(pair.Key, start) > 0
                        : comparer.Compare(pair.Key, start) >= 0);
                }
                if (end is not null)
                {
                    candidates = candidates.Where(pair => comparer.Compare(pair.Key, end) < 0);
                }
            }

            var cap = Math.Min(BrokerMaxScanItems, _scanPageSize ?? BrokerMaxScanItems);
            var limit = query.Limit is null or 0 ? cap : (int)Math.Min(query.Limit.Value, (uint)cap);
            var pairs = new List<KvPair>();
            long used = 0;
            var hasMore = false;
            foreach (var pair in candidates)
            {
                if (pairs.Count >= limit)
                {
                    hasMore = true;
                    break;
                }

                var cost = BrokerScanItemOverheadBytes + (long)pair.Key.Length + pair.Value.Length;
                if (used + cost > BrokerScanResponseCeilingBytes)
                {
                    if (pairs.Count == 0)
                    {
                        throw BrokerError(
                            "SCAN",
                            InvalidRequestCode,
                            $"Invalid request: scan pair ({pair.Key.Length} byte key) is {cost} wire bytes, " +
                            $"exceeding the {BrokerScanResponseCeilingBytes}-byte limit a scan response can return");
                    }
                    hasMore = true;
                    break;
                }
                used += cost;
                pairs.Add(new KvPair(pair.Key.ToArray(), pair.Value.ToArray()));
            }
            return new KvScanResult(pairs, hasMore);
        }

        long ScanRequestBytes(KvScanQuery query)
        {
            // has_start, has_end, has_limit and the options byte, plus optional bodies.
            var bytes = _requestPrefixBytes + 4;
            if (query.StartKey is { } start)
            {
                bytes += sizeof(int) + start.Length;
            }
            if (query.EndKey is { } end)
            {
                bytes += sizeof(int) + end.Length;
            }
            return query.Limit.HasValue ? bytes + sizeof(uint) : bytes;
        }

        void EnsureOpen()
        {
            if (_closed)
            {
                throw new KvException("The transaction is closed.", "TX_CLOSED");
            }
            _owner.EnsureGeneration(Generation);
        }
    }
}
