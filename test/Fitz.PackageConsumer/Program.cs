using Cntryl.Fitz;
using Cntryl.Fitz.Abstractions;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();
services.AddFitzClient(new ClientConfig(new Uri("ws://localhost:4190/ws")));

Console.WriteLine(typeof(Client).FullName);
Console.WriteLine(typeof(IKvClient).FullName);
Console.WriteLine(typeof(ServiceCollectionExtensions).FullName);

foreach (var assembly in new[] { typeof(Client).Assembly, typeof(IKvClient).Assembly, typeof(ServiceCollectionExtensions).Assembly })
{
    var name = assembly.GetName();
    if (name.Version != new Version(1, 0, 0, 0))
    {
        throw new InvalidOperationException($"{name.Name} assembly version must remain 1.0.0.0; found {name.Version}.");
    }
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
    CancellationToken cancellationToken)
{
    var subscription = await notice.SubscribeAsync(
        "notice://example/app/*",
        cancellationToken).ConfigureAwait(false);
    await using var configuredSubscription = ((IAsyncDisposable)subscription).ConfigureAwait(false);
    await foreach (var notification in subscription
        .WithCancellation(cancellationToken)
        .ConfigureAwait(false))
    {
        Console.WriteLine(notification.Route);
        break;
    }

    var result = await schedule.ListAsync(ct: cancellationToken).ConfigureAwait(false);
    Console.WriteLine($"{result.Entries.Count}/{result.TotalCount}");
}

Func<INoticeClient, IScheduleClient, CancellationToken, Task> previewApi = CompilePreviewApiAsync;
GC.KeepAlive(previewApi);

static async Task CompileStreamSelectorsAsync(
    IStreamClient stream,
    CancellationToken cancellationToken)
{
    await foreach (var record in stream.ReadAsync(
        "stream://example/app/*",
        0,
        ct: cancellationToken).ConfigureAwait(false))
    {
        Console.WriteLine(record.Route);
    }

    var page = await stream.ReadPageAsync(
        "stream://**",
        0,
        ct: cancellationToken).ConfigureAwait(false);
    Console.WriteLine(page.Items.Count);
}

Func<IStreamClient, CancellationToken, Task> streamSelectors = CompileStreamSelectorsAsync;
GC.KeepAlive(streamSelectors);

static async Task<ulong> CompileManagedLeaseAuthorityAsync(
    ILeaseClient lease,
    CancellationToken cancellationToken)
{
    var fencingToken = await lease.WithLeaseAsync(
        "lease://example/app/leader",
        30,
        static (authority, callbackCancellationToken) =>
        {
            callbackCancellationToken.ThrowIfCancellationRequested();
            var exactFencingToken = authority.FencingToken;
            return ValueTask.FromResult(exactFencingToken);
        },
        ct: cancellationToken).ConfigureAwait(false);

    await lease.WithLeaseAsync(
        "lease://example/app/legacy",
        30,
        static _ => ValueTask.CompletedTask,
        ct: cancellationToken).ConfigureAwait(false);
    return fencingToken;
}

Func<ILeaseClient, CancellationToken, Task<ulong>> managedLeaseApi = CompileManagedLeaseAuthorityAsync;
GC.KeepAlive(managedLeaseApi);
