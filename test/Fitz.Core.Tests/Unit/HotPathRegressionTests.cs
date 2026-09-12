using System.Buffers;
using System.Runtime.InteropServices;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

/// <summary>
/// Guards the behavioural contracts that the receive-path allocation work depends on: pooled
/// buffers must still be cleared over the region they held, and batching notification fan-out into
/// one queue entry per frame must not change delivery or isolation.
/// </summary>
public sealed class HotPathRegressionTests
{
    [Fact]
    public void ShouldClearWrittenRegionGivenPooledFrameWhenDisposed()
    {
        // Arrange
        var buffer = ArrayPool<byte>.Shared.Rent(256);
        buffer.AsSpan().Fill(0xAB);
        var frame = PooledFrame.FromRentedBuffer(buffer, 64);

        // Act
        frame.Dispose();

        // Assert
        Assert.True(
            buffer.AsSpan(0, 64).IndexOfAnyExcept((byte)0) < 0,
            "PooledFrame must zero the bytes it exposed before returning the buffer to the pool.");
    }

    [Fact]
    public void ShouldClearWrittenRegionGivenBufferWriterWhenDisposed()
    {
        // Arrange
        var writer = new BinaryBufferWriter();
        writer.WriteU64(0xDEADBEEFCAFEF00D);
        writer.WriteString("kv://realm/app/users");
        var written = writer.WrittenCount;
        var rented = writer.WrittenMemory;
        Assert.True(MemoryMarshal.TryGetArray(rented, out var segment));
        var backing = segment.Array!;

        // Act
        writer.Dispose();

        // Assert
        Assert.True(
            backing.AsSpan(0, written).IndexOfAnyExcept((byte)0) < 0,
            "BinaryBufferWriter must zero the bytes it wrote before returning the buffer to the pool.");
    }

    [Fact]
    public async Task ShouldDeliverToEverySubscriberGivenManyHandlersWhenOneNotificationDispatched()
    {
        // Arrange
        const int subscribers = 32;
        using var mux = new Multiplexer();
        mux.SetConnected();
        var countdown = new CountdownEvent(subscribers);
        var registrations = new List<IDisposable>();
        for (var i = 0; i < subscribers; i++)
        {
            registrations.Add(mux.RegisterNotificationHandler(
                MessageTypes.NoticeNotify,
                _ => countdown.Signal()));
        }

        // Act
        mux.Dispatch(MessageTypes.NoticeNotify, new byte[] { 0x1, 0x2 });

        // Assert
        Assert.True(await Task.Run(() => countdown.Wait(TimeSpan.FromSeconds(10))));
        foreach (var registration in registrations)
        {
            registration.Dispose();
        }

        countdown.Dispose();
    }

    [Fact]
    public async Task ShouldInvokeRemainingSubscribersGivenThrowingHandlerWhenNotificationDispatched()
    {
        // Arrange
        var reported = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var mux = new Multiplexer(onDispatchError: ex => reported.TrySetResult(ex));
        mux.SetConnected();
        var afterThrowing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var throwing = mux.RegisterNotificationHandler(
            MessageTypes.NoticeNotify,
            _ => throw new InvalidOperationException("handler failed"));
        using var healthy = mux.RegisterNotificationHandler(
            MessageTypes.NoticeNotify,
            _ => afterThrowing.TrySetResult());

        // Act
        mux.Dispatch(MessageTypes.NoticeNotify, new byte[] { 0x7 });

        // Assert
        await afterThrowing.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var failure = await reported.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("handler failed", failure.Message);
    }

    [Fact]
    public async Task ShouldStopDeliveryGivenUnregisteredHandlerWhenLaterNotificationDispatched()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var removedCalls = 0;
        var kept = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var removed = mux.RegisterNotificationHandler(
            MessageTypes.NoticeNotify,
            _ => Interlocked.Increment(ref removedCalls));
        using var survivor = mux.RegisterNotificationHandler(
            MessageTypes.NoticeNotify,
            _ => kept.TrySetResult());

        // Act
        removed.Dispose();
        mux.Dispatch(MessageTypes.NoticeNotify, new byte[] { 0x3 });

        // Assert
        await kept.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(0, Volatile.Read(ref removedCalls));
    }

    [Fact]
    public async Task ShouldServeSecondRequestGivenSameMessageTypeWhenFirstLaneReleased()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        // Act
        var first = mux.RequestAsync(
            MessageTypes.KvGet,
            [0x1],
            (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5));
        mux.Dispatch(MessageTypes.KvGet, new byte[] { 0xA });
        var firstResponse = await first.WaitAsync(TimeSpan.FromSeconds(10));

        var second = mux.RequestAsync(
            MessageTypes.KvGet,
            [0x2],
            (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5));
        mux.Dispatch(MessageTypes.KvGet, new byte[] { 0xB });
        var secondResponse = await second.WaitAsync(TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal([0xA], firstResponse);
        Assert.Equal([0xB], secondResponse);
    }
}
