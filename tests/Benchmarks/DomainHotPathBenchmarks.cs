using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Domains.Notice;
using Cntryl.Fitz.Domains.Queue;
using Cntryl.Fitz.Domains.Schedule;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Benchmarks;

[SimpleJob]
[MemoryDiagnoser]
[PlainExporter]
internal sealed class DomainHotPathBenchmarks : IDisposable
{
    private static readonly byte[] Payload = "payload"u8.ToArray();

    private KvClient _kv = null!;
    private QueueClient _queue = null!;
    private LeaseClient _lease = null!;
    private NoticeClient _notice = null!;
    private ScheduleClient _schedule = null!;

    [GlobalSetup]
    public void Setup()
    {
        _kv = new KvClient(KvRequest);
        _queue = new QueueClient(QueueRequest);
        _lease = new LeaseClient(LeaseRequest);
        _notice = new NoticeClient(NoticeSend);
        _schedule = new ScheduleClient(ScheduleRequest);
    }

    [GlobalCleanup]
    public void Dispose()
    {
        _kv.Dispose();
        _queue.Dispose();
        _lease.Dispose();
        _notice.Dispose();
        _schedule.Dispose();
    }

    [Benchmark]
    public async Task KvBeginGet()
    {
        var tx = await _kv.BeginAsync("kv://bench/hotpath", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async).ConfigureAwait(false);
        _ = await tx.GetAsync(Payload).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task<ulong> QueueEnqueue()
    {
        return await _queue.EnqueueAsync("queue://bench/hotpath", Payload).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task<ILease> LeaseAcquire()
    {
        return await _lease.AcquireAsync("lease://bench/hotpath", 30).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task NoticePublish()
    {
        await _notice.PublishAsync("notice://bench/hotpath", Payload).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task<string?> ScheduleCreate()
    {
        return await _schedule.CreateAsync("schedule://bench/hotpath", "*/1 * * * *", ScheduleDeliveryMode.Broadcast, Payload).ConfigureAwait(false);
    }

    private static Task<byte[]> KvRequest(ushort messageType, byte[] _, CancellationToken __)
    {
        using var writer = new BinaryBufferWriter();
        if (messageType == MessageTypes.KvBegin)
        {
            writer.WriteU8(0);
            writer.WriteU64(1);
            return Task.FromResult(writer.Build());
        }

        if (messageType == MessageTypes.KvGet)
        {
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteU32((uint)Payload.Length);
            writer.WriteBytes(Payload);
            return Task.FromResult(writer.Build());
        }

        writer.WriteU8(0);
        return Task.FromResult(writer.Build());
    }

    private static Task<byte[]> QueueRequest(ushort messageType, byte[] _, CancellationToken __)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(0);
        if (messageType == MessageTypes.QueueEnqueue)
        {
            writer.WriteU64(1);
        }

        return Task.FromResult(writer.Build());
    }

    private static Task<byte[]> LeaseRequest(ushort messageType, byte[] _, CancellationToken __)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(0);
        if (messageType == MessageTypes.LeaseAcquire)
        {
            writer.WriteU8(1);
            writer.WriteU64(1);
        }

        return Task.FromResult(writer.Build());
    }

    private static Task NoticeSend(ushort _, byte[] __, CancellationToken ___)
    {
        return Task.CompletedTask;
    }

    private static Task<byte[]> ScheduleRequest(ushort messageType, byte[] _, CancellationToken __)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(0);
        if (messageType == MessageTypes.ScheduleCreate)
        {
            writer.WriteU8(1);
            writer.WriteString("sched-1");
        }

        return Task.FromResult(writer.Build());
    }
}
