using Cntryl.Fitz;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();
services.AddFitzClient(new ClientConfig(new Uri("ws://localhost:4190/ws")));

Console.WriteLine(typeof(Client).FullName);
Console.WriteLine(typeof(IKvClient).FullName);
Console.WriteLine(typeof(ServiceCollectionExtensions).FullName);

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

    ScheduleListResult result = await schedule.ListAsync(ct: cancellationToken).ConfigureAwait(false);
    Console.WriteLine($"{result.Entries.Count}/{result.TotalCount}");
}

Func<INoticeClient, IScheduleClient, CancellationToken, Task> previewApi = CompilePreviewApiAsync;
GC.KeepAlive(previewApi);
