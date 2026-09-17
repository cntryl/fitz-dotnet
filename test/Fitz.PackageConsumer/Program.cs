using Cntryl.Fitz;
using Cntryl.Fitz.DependencyInjection;
using Cntryl.Fitz.Testing;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();
services.AddFitzClient(new ClientConfig(new Uri("ws://localhost:4190/ws")));

Console.WriteLine(typeof(Client).FullName);
Console.WriteLine(typeof(IKvClient).FullName);
Console.WriteLine(typeof(ServiceCollectionExtensions).FullName);
Console.WriteLine(typeof(InMemoryKvClient).FullName);

foreach (var assembly in new[] { typeof(Client).Assembly, typeof(IKvClient).Assembly, typeof(ServiceCollectionExtensions).Assembly, typeof(InMemoryKvClient).Assembly })
{
    var name = assembly.GetName();
    if (name.Version != new Version(1, 0, 0, 0))
    {
        throw new InvalidOperationException($"{name.Name} assembly version must remain 1.0.0.0; found {name.Version}.");
    }
}

var inMemoryKv = new InMemoryKvClient(new InMemoryKvClientOptions { ScanPageSize = 1 });
await using (var transaction = await inMemoryKv.BeginAsync(
    "kv://consumer/package/smoke",
    KvDurability.Async))
{
    await transaction.PutAsync("key"u8.ToArray(), "value"u8.ToArray());
    await transaction.CommitAsync();
}
if (!inMemoryKv.Snapshot("kv://consumer/package/smoke").Single().Value.Span.SequenceEqual("value"u8))
{
    throw new InvalidOperationException("The packed in-memory KV client did not commit its value.");
}

uint[] rpcErrorCodes =
[
    FitzErrorCodes.RpcTimeout,
    FitzErrorCodes.RpcWorkerNotFound,
    FitzErrorCodes.RpcBackpressure,
    FitzErrorCodes.RpcRouteNotRegistered,
    FitzErrorCodes.RpcCorrelationNotFound,
    FitzErrorCodes.RpcInvalidSequence,
    FitzErrorCodes.RpcDuplicateCorrelation,
    FitzErrorCodes.RpcWrongWorker,
    FitzErrorCodes.RpcUnauthorized,
    FitzErrorCodes.RpcBackendError,
    FitzErrorCodes.RpcInvalidRoute,
    FitzErrorCodes.RpcInvalidSubscriptionPattern,
    FitzErrorCodes.RpcSubscriptionLimit,
];
if (!rpcErrorCodes.SequenceEqual(Enumerable.Range(6001, 13).Select(static code => (uint)code)))
{
    throw new InvalidOperationException("RPC error code constants must cover the canonical 6001-6013 range.");
}


static async Task CompilePreviewApiAsync(
    INoticeClient notice,
    IScheduleClient schedule,
    CancellationToken ct)
{
    var subscription = await notice.SubscribeAsync(
        "notice://example/app/*",
        ct).ConfigureAwait(false);
    await using var configuredSubscription = ((IAsyncDisposable)subscription).ConfigureAwait(false);
    await foreach (var notification in subscription
        .WithCancellation(ct)
        .ConfigureAwait(false))
    {
        Console.WriteLine(notification.Route);
        break;
    }

    var result = await schedule.ListAsync(ct: ct).ConfigureAwait(false);
    Console.WriteLine($"{result.Entries.Count}/{result.TotalCount}");
}

Func<INoticeClient, IScheduleClient, CancellationToken, Task> previewApi = CompilePreviewApiAsync;
GC.KeepAlive(previewApi);

static async Task CompileStreamSelectorsAsync(
    IStreamClient stream,
    CancellationToken ct)
{
    await foreach (var record in stream.ReadAsync(
        "stream://example/app/*",
        0,
        ct: ct).ConfigureAwait(false))
    {
        Console.WriteLine(record.Route);
    }

    var page = await stream.ReadPageAsync(
        "stream://**",
        0,
        ct: ct).ConfigureAwait(false);
    Console.WriteLine(page.Items.Count);
}

Func<IStreamClient, CancellationToken, Task> streamSelectors = CompileStreamSelectorsAsync;
GC.KeepAlive(streamSelectors);

static async Task<ulong> CompileManagedLeaseAuthorityAsync(
    ILeaseClient lease,
    CancellationToken ct)
{
    var fencingToken = await lease.WithLeaseAsync(
        "lease://example/app/leader",
        TimeSpan.FromSeconds(30),
        static (authority, callbackCancellationToken) =>
        {
            callbackCancellationToken.ThrowIfCancellationRequested();
            var exactFencingToken = authority.FencingToken;
            return ValueTask.FromResult(exactFencingToken);
        },
        ct: ct).ConfigureAwait(false);

    await lease.WithLeaseAsync(
        "lease://example/app/legacy",
        TimeSpan.FromSeconds(30),
        static _ => ValueTask.CompletedTask,
        ct: ct).ConfigureAwait(false);
    return fencingToken;
}

Func<ILeaseClient, CancellationToken, Task<ulong>> managedLeaseApi = CompileManagedLeaseAuthorityAsync;
GC.KeepAlive(managedLeaseApi);
