using Cntryl.Fitz.Abstractions;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class LeaseClientTests
{
    [Fact]
    public async Task ShouldReturnTypedErrorGivenEmptyResponseWhenQueryingLease()
    {
        // Arrange
        // Act
        // Assert
        using var leaseClient = new LeaseClient((_, _, _) => Task.FromResult(Array.Empty<byte>()));

        var error = await Assert.ThrowsAsync<LeaseException>(() =>
            leaseClient.QueryAsync("lease://prod/app/lock"));

        Assert.Equal("QUERY_INVALID_RESPONSE", error.Code);
    }

    [Fact]
    public async Task ShouldRejectZeroTtlGivenLeaseHandleWhenExtendingBeforeTransport()
    {
        // Arrange
        var requestCalled = false;

        // Act
        await using var lease = new LeaseHandle(
            (_, _, _) =>
            {
                requestCalled = true;
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(Array.Empty<byte>());
            },
            "lease://prod/app/lock",
            77);


        // Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => lease.ExtendAsync(0));

        Assert.False(requestCalled);
    }

    [Fact]
    public async Task ShouldRejectInvalidHolderFlagGivenMalformedResponseWhenQueryingLease()
    {
        // Arrange
        // Act
        // Assert
        using var leaseClient = new LeaseClient((_, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(2);
            writer.WriteString("owner");
            writer.WriteU64(30);
            writer.WriteU32(0);
            return Task.FromResult(writer.Build());
        });

        var error = await Assert.ThrowsAsync<LeaseException>(() =>
            leaseClient.QueryAsync("lease://prod/app/lock"));

        Assert.Equal("QUERY_INVALID_RESPONSE", error.Code);
    }

    [Fact]
    public async Task ShouldRejectZeroTtlGivenAcquireWhenBeforeTransport()
    {
        // Arrange
        var requestCalled = false;

        // Act
        using var leaseClient = new LeaseClient((_, _, _) =>
        {
            requestCalled = true;
            return Task.FromResult(Array.Empty<byte>());
        });


        // Assert
        var error = await Assert.ThrowsAsync<LeaseException>(() =>
            leaseClient.AcquireAsync("lease://prod/app/lock", 0));

        Assert.Equal("INVALID_TTL", error.Code);
        Assert.False(requestCalled);
    }

    [Fact]
    public async Task ShouldReleaseDisconnectRegistrationGivenCleanupFailureWhenDisposingLease()
    {
        // Arrange
        var registrations = 0;
        await using var lease = new LeaseHandle(
            (_, _, _) => ValueTask.FromException<ReadOnlyMemory<byte>>(new LeaseException("release failed", "RELEASE_FAILED")),
            "lease://prod/app/lock",
            77,
            _ =>
            {
                registrations++;
                return new TestRegistration(() => registrations--);
            });


        // Act
        await lease.DisposeAsync();


        // Assert
        Assert.Equal(0, registrations);
        var error = await Assert.ThrowsAsync<LeaseException>(() => lease.ExtendAsync(30));
        Assert.Equal("CLOSED", error.Code);
    }

    [Fact]
    public async Task ShouldRecordRotationGivenChangedFencingTokenWhenRenewingLease()
    {
        // Arrange
        await using var lease = new LeaseHandle(
            (_, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteU64(78);
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build());
            },
            "lease://prod/app/lock",
            77);


        // Act
        await lease.ExtendAsync(30);


        // Assert
        Assert.Equal(78UL, lease.FencingToken);
        Assert.True(lease.FencingTokenChanged.IsCompleted);
    }

    [Fact]
    public async Task ShouldAllowReleaseRetryAfterRejectedWireOperationGivenLeaseClientWhenOperationRuns()
    {
        // Arrange
        var releaseCalls = 0;
        using var leaseClient = new LeaseClient((messageType, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            if (messageType == MessageTypes.LeaseAcquire)
            {
                writer.WriteU8(0);
                writer.WriteU8(1);
                writer.WriteU64(77);
            }
            else if (Interlocked.Increment(ref releaseCalls) == 1)
            {
                writer.WriteU8(1);
                writer.WriteU32(5009);
                writer.WriteString("try again");
            }
            else
            {
                writer.WriteU8(0);
            }

            return Task.FromResult(writer.Build());
        });

        // Act
        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);


        // Assert
        await Assert.ThrowsAsync<LeaseException>(() => lease.ReleaseAsync());
        await lease.ReleaseAsync();
        var closed = await Assert.ThrowsAsync<LeaseException>(() => lease.ReleaseAsync());

        Assert.Equal("CLOSED", closed.Code);
        Assert.Equal(2, releaseCalls);
    }

    [Fact]
    public async Task ShouldPreserveDomainCodeGivenTypedErrorWhenAcquiringLease()
    {
        // Arrange
        using var leaseClient = new LeaseClient((_, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(1);
            writer.WriteU32(FitzErrorCodes.LeaseHeld);
            writer.WriteString("HeldByOther: worker-1");
            return Task.FromResult(writer.Build());
        });

        // Act
        var act = () => leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        // Assert
        var error = await Assert.ThrowsAsync<LeaseException>(act);
        Assert.Equal(FitzErrorCodes.LeaseHeld, error.DomainCode);
    }

    [Fact]
    public async Task ShouldReleaseOnDisposeGivenCancelledExtendWhenLeaseOperationRuns()
    {
        // Arrange
        var releaseCalls = 0;
        using var leaseClient = new LeaseClient((messageType, _, ct) =>
        {
            if (messageType == MessageTypes.LeaseAcquire)
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteU8(1);
                writer.WriteU64(77);
                return Task.FromResult(writer.Build());
            }

            if (messageType == MessageTypes.LeaseRenew)
            {
                return Task.FromCanceled<byte[]>(ct);
            }

            Interlocked.Increment(ref releaseCalls);
            return Task.FromResult(new byte[] { 0 });
        });

        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);
        using var cancellation = new CancellationTokenSource();

        // Act
        await cancellation.CancelAsync();

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => lease.ExtendAsync(60, cancellation.Token));
        await lease.DisposeAsync();

        Assert.Equal(1, releaseCalls);
    }

    [Fact]
    public async Task ShouldAwaitDeferredAcquiredFrameGivenQueuedResponseWhenLeaseOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        Action<byte[]>? acquireHandler = null;
        using var leaseClient = new LeaseClient(
            (_, _, _) =>
            {
                using var queued = new BinaryBufferWriter();
                queued.WriteU8(0);
                queued.WriteU8(2);
                queued.WriteU64(0);
                return Task.FromResult(queued.Build());
            },
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.LeaseAcquire, messageType);
                acquireHandler = handler;
                return new TestRegistration();
            });

        var pending = leaseClient.AcquireAsync("lease://prod/app/lock", 30, waitSeconds: 5);
        await Task.Yield();
        using var acquired = new BinaryBufferWriter();
        acquired.WriteU8(0);
        acquired.WriteU8(0);
        acquired.WriteU64(91);
        acquireHandler!(acquired.Build());

        var lease = await pending;
        Assert.Equal("lease://prod/app/lock", lease.Route);
    }

    [Fact]
    public async Task ShouldCopyPayloadGivenDeferredGrantWhenBorrowedNotificationReturns()
    {
        // Arrange
        Action<ReadOnlyMemory<byte>>? acquireHandler = null;
        using var leaseClient = new LeaseClient(
            (_, _, _) =>
            {
                using var queued = new BinaryBufferWriter();
                queued.WriteU8(0);
                queued.WriteU8(2);
                queued.WriteU64(0);
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(queued.Build());
            },
            registerNotificationHandler: (_, handler) =>
            {
                acquireHandler = handler;
                return new TestRegistration();
            });

        var pending = leaseClient.AcquireAsync("lease://prod/app/lock", 30, waitSeconds: 5);
        await Task.Yield();
        using var acquired = new BinaryBufferWriter();
        acquired.WriteU8(0);
        acquired.WriteU8(0);
        acquired.WriteU64(91);
        var borrowed = acquired.Build();
        acquireHandler!(borrowed);
        borrowed.AsSpan().Fill(byte.MaxValue);


        // Act
        await using var lease = await pending;

        // Assert
        Assert.Equal(91UL, lease.FencingToken);
    }

    [Fact]
    public async Task ShouldSerializeAcquisitionLifecycleUntilDeferredFrameArrivesGivenLeaseClientWhenOperationRuns()
    {
        // Arrange
        Action<byte[]>? acquireHandler = null;

        // Act
        var acquireCalls = 0;

        // Assert
        using var leaseClient = new LeaseClient(
            (messageType, _, _) =>
            {
                Assert.Equal(MessageTypes.LeaseAcquire, messageType);
                using var response = new BinaryBufferWriter();
                response.WriteU8(0);
                response.WriteU8(Interlocked.Increment(ref acquireCalls) == 1 ? (byte)2 : (byte)0);
                response.WriteU64(0);
                return Task.FromResult(response.Build());
            },
            (_, handler) =>
            {
                acquireHandler = handler;
                return new TestRegistration();
            });

        var first = leaseClient.AcquireAsync("lease://prod/app/first", 30, waitSeconds: 5);
        var second = leaseClient.AcquireAsync("lease://prod/app/second", 30);
        await Task.Yield();
        Assert.Equal(1, Volatile.Read(ref acquireCalls));

        using var acquired = new BinaryBufferWriter();
        acquired.WriteU8(0);
        acquired.WriteU8(0);
        acquired.WriteU64(91);
        acquireHandler!(acquired.Build());

        await first;
        await second;
        Assert.Equal(2, Volatile.Read(ref acquireCalls));
    }

    [Fact]
    public async Task ShouldDiscardDeferredGrantGivenTimedOutAcquireWhenNextCallerWaits()
    {
        // Arrange
        Action<byte[]>? acquireHandler = null;
        var acquireCalls = 0;

        // Act
        using var leaseClient = new LeaseClient(
            (_, _, _) =>
            {
                Interlocked.Increment(ref acquireCalls);
                using var queued = new BinaryBufferWriter();
                queued.WriteU8(0);
                queued.WriteU8(2);
                queued.WriteU64(0);
                return Task.FromResult(queued.Build());
            },
            (_, handler) =>
            {
                acquireHandler = handler;
                return new TestRegistration();
            });


        // Assert
        await Assert.ThrowsAsync<RequestTimeoutException>(() =>
            leaseClient.AcquireAsync("lease://prod/app/first", 30, waitSeconds: 1));

        var second = leaseClient.AcquireAsync("lease://prod/app/second", 30, waitSeconds: 5);
        using var lateGrant = new BinaryBufferWriter();
        lateGrant.WriteU8(0);
        lateGrant.WriteU8(0);
        lateGrant.WriteU64(91);
        acquireHandler!(lateGrant.Build());

        await Assert.ThrowsAsync<ConnectionException>(() => second);
        Assert.Equal(1, Volatile.Read(ref acquireCalls));
    }

    [Fact]
    public async Task ShouldReturnLeaseHandleGivenSuccessResponseWhenAcquiringLease()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        using var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteU64(77);
            return Task.FromResult(writer.Build());
        });

        // Act
        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30, waitSeconds: 5);

        // Assert
        Assert.Equal(MessageTypes.LeaseAcquire, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal("lease://prod/app/lock", lease.Route);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("lease://prod/app/lock", reader.ReadString());
        Assert.Equal(string.Empty, reader.ReadString());
        Assert.Equal((ulong)30, reader.ReadU64());
        Assert.Equal((uint)5, reader.ReadU32());
        Assert.True(reader.IsEof);
    }

    [Fact]
    public async Task ShouldReturnHeldLeaseInfoGivenHolderPresentWhenQueryingLease()
    {
        // Arrange
        using var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.LeaseQuery, messageType);

            var request = new BinaryBufferReader(payload);
            Assert.Equal("lease://prod/app/lock", request.ReadString());

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteString("worker-1");
            writer.WriteU64(18);
            writer.WriteU32(0);
            return Task.FromResult(writer.Build());
        });

        // Act
        var info = await leaseClient.QueryAsync("lease://prod/app/lock");

        // Assert
        Assert.True(info.IsHeld);
        Assert.Equal("worker-1", info.Owner);
        Assert.Equal((ulong)18, info.TtlRemainingSecs);
    }

    [Fact]
    public async Task ShouldExposePendingWaitersGivenSuccessResponseWhenQueryingLease()
    {
        // Arrange
        using var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.LeaseQuery, messageType);

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteString("worker-1");
            writer.WriteU64(18);
            writer.WriteU32(3);
            return Task.FromResult(writer.Build());
        });

        // Act
        var info = await leaseClient.QueryAsync("lease://prod/app/lock");

        // Assert
        Assert.True(info.IsHeld);
        Assert.Equal("worker-1", info.Owner);
        Assert.Equal((ulong)18, info.TtlRemainingSecs);
        Assert.Equal((uint)3, info.PendingWaiters);
    }

    [Fact]
    public async Task ShouldEncodeTtlGivenLeaseHandleWhenExtendingLease()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();

        using var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (messageType == MessageTypes.LeaseAcquire)
            {
                writer.WriteU8(1);
                writer.WriteU64(77);
            }
            else if (messageType == MessageTypes.LeaseRenew)
            {
                writer.WriteU64(77);
            }

            return Task.FromResult(writer.Build());
        });

        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        // Act
        await lease.ExtendAsync(45);

        // Assert
        Assert.Equal(2, calls.Count);
        Assert.Equal(MessageTypes.LeaseAcquire, calls[0].MessageType);
        Assert.Equal(MessageTypes.LeaseRenew, calls[1].MessageType);

        var extendReader = new BinaryBufferReader(calls[1].Payload);
        Assert.Equal("lease://prod/app/lock", extendReader.ReadString());
        Assert.Equal(string.Empty, extendReader.ReadString());
        Assert.Equal((ulong)77, extendReader.ReadU64());
        Assert.Equal((ulong)45, extendReader.ReadU64());
    }

    [Fact]
    public async Task ShouldEncodeTokenGivenLeaseHandleWhenReleasingLease()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();

        using var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (messageType == MessageTypes.LeaseAcquire)
            {
                writer.WriteU8(1);
                writer.WriteU64(77);
            }

            return Task.FromResult(writer.Build());
        });

        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        // Act
        await lease.ReleaseAsync();

        // Assert
        Assert.Equal(2, calls.Count);
        Assert.Equal(MessageTypes.LeaseAcquire, calls[0].MessageType);
        Assert.Equal(MessageTypes.LeaseRelease, calls[1].MessageType);

        var releaseReader = new BinaryBufferReader(calls[1].Payload);
        Assert.Equal("lease://prod/app/lock", releaseReader.ReadString());
        Assert.Equal(string.Empty, releaseReader.ReadString());
        Assert.Equal((ulong)77, releaseReader.ReadU64());
    }

    [Fact]
    public async Task ShouldReleaseOnceGivenRepeatedAsyncDisposalOfLiveLeaseWhenLeaseOperationRuns()
    {
        // Arrange
        var releaseCalls = 0;
        using var leaseClient = new LeaseClient((messageType, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (messageType == MessageTypes.LeaseAcquire)
            {
                writer.WriteU8(1);
                writer.WriteU64(77);
            }
            else if (messageType == MessageTypes.LeaseRelease)
            {
                releaseCalls++;
            }

            return Task.FromResult(writer.Build());
        });
        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        // Act
        await lease.DisposeAsync();
        await lease.DisposeAsync();

        // Assert
        Assert.Equal(1, releaseCalls);
    }

    [Fact]
    public async Task ShouldNotSurfaceReleaseFailureGivenAsyncDisposalOfLiveLeaseWhenLeaseOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        using var leaseClient = new LeaseClient((messageType, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(messageType == MessageTypes.LeaseRelease ? (byte)1 : (byte)0);
            if (messageType == MessageTypes.LeaseAcquire)
            {
                writer.WriteU8(1);
                writer.WriteU64(77);
            }
            else
            {
                writer.WriteU32(9001);
                writer.WriteString("release failed");
            }

            return Task.FromResult(writer.Build());
        });
        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        // Act
        await lease.DisposeAsync();
    }

    [Fact]
    public async Task ShouldInvokeLeaseHandlerGivenNotificationWhenSubscribing()
    {
        // Arrange
        Action<byte[]>? notifyHandler = null;
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;
        LeaseChangeEvent? received = null;
        CancellationToken seenCancellationToken = default;
        var receivedTcs = new TaskCompletionSource<LeaseChangeEvent>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var leaseClient = new LeaseClient(
            (messageType, payload, _) =>
            {
                seenMessageType = messageType;
                seenPayload = payload;

                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                if (messageType == MessageTypes.LeaseSubscribe)
                {
                    writer.WriteU64(555);
                }
                return Task.FromResult(writer.Build());
            },
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.LeaseNotify, messageType);
                notifyHandler = handler;
                return new TestRegistration();
            });

        // Act
        var subscription = await leaseClient.SubscribeAsync("lease://prod/app/lock", (evt, cancellationToken) =>
        {
            received = evt;
            seenCancellationToken = cancellationToken;
            receivedTcs.TrySetResult(evt);
            return ValueTask.CompletedTask;
        });

        await Task.Delay(25);
        Assert.NotNull(notifyHandler);
        using var notification = new BinaryBufferWriter();
        const ulong subscriptionId = 555;
        notification.WriteU64(subscriptionId);
        notification.WriteString("lease://prod/app/lock");
        notification.WriteU32(0);
        notifyHandler!(notification.Build());

        var evt = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.NotNull(evt);
        Assert.Equal(MessageTypes.LeaseSubscribe, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal("lease://prod/app/lock", evt!.Route);
        Assert.Same(received, evt);
        Assert.NotEqual(default, seenCancellationToken);
        Assert.False(seenCancellationToken.IsCancellationRequested);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("lease://prod/app/lock", reader.ReadString());

        await subscription.DisposeAsync();
    }

    [Fact]
    public async Task ShouldMarkLeaseAsClosedAfterDisconnectGivenLeaseClientWhenOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        await using var transport = new TestQueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 1)
            {
                using var authProbeWriter = new BinaryBufferWriter();
                authProbeWriter.WriteU8(0);
                transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, authProbeWriter.WrittenSpan));
            }
            else if (sentFrameCount == 2)
            {
                using var acquireWriter = new BinaryBufferWriter();
                acquireWriter.WriteU8(0);
                acquireWriter.WriteU8(1);
                acquireWriter.WriteU64(77);
                transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseAcquire, acquireWriter.WrittenSpan));
            }
        };

        var config = new ClientConfig(new Uri("ws://localhost:4190/ws"), TransportFactory: _ => transport);
        await using var connection = new FitzConnection(config, () => transport);
        using var leaseClient = new LeaseClient(connection);

        await connection.ConnectAsync();
        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        await connection.CloseAsync();

        var ex = await Assert.ThrowsAsync<LeaseException>(() => lease.ExtendAsync(60));

        // Assert
        Assert.Equal("CLOSED", ex.Code);
        Assert.Equal("Lease handle is no longer valid after disconnect", ex.Message);
    }

    [Fact]
    public async Task ShouldRejectExtendGivenStaleHandleWhenReconnectCompletes()
    {
        // Arrange
        await using var firstTransport = new TestQueuedTransport();
        await using var secondTransport = new TestQueuedTransport();
        var reconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        firstTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 1)
            {
                using var authProbeWriter = new BinaryBufferWriter();
                authProbeWriter.WriteU8(0);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, authProbeWriter.WrittenSpan));
            }
            else if (sentFrameCount == 2)
            {
                using var acquireWriter = new BinaryBufferWriter();
                acquireWriter.WriteU8(0);
                acquireWriter.WriteU8(1);
                acquireWriter.WriteU64(77);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseAcquire, acquireWriter.WrittenSpan));
            }
        };

        secondTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var authProbeWriter = new BinaryBufferWriter();
            authProbeWriter.WriteU8(0);
            secondTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, authProbeWriter.WrittenSpan));
            reconnected.TrySetResult();
        };

        var transportFactoryCalls = 0;
        Func<ITransport> transportFactory = () => transportFactoryCalls++ == 0 ? firstTransport : secondTransport;
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(10), MaxBackoff: TimeSpan.FromMilliseconds(10))),
            transportFactory);
        using var leaseClient = new LeaseClient(connection);

        await connection.ConnectAsync();
        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        firstTransport.QueueClosed();

        // Act
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(1));


        // Assert
        var ex = await Assert.ThrowsAsync<LeaseException>(() => lease.ExtendAsync(60));

        Assert.Equal("CLOSED", ex.Code);
        Assert.Equal("Lease handle is no longer valid after disconnect", ex.Message);

        await connection.CloseAsync();
    }

    [Fact]
    public async Task ShouldAcceptWholeSegmentWildcardRouteGivenValidPatternWhenSubscribing()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        using var leaseClient = new LeaseClient(
            (messageType, payload, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                if (messageType == MessageTypes.LeaseSubscribe)
                {
                    seenMessageType = messageType;
                    seenPayload = payload;
                    writer.WriteU64(555);
                }
                return Task.FromResult(writer.Build());
            },
            (messageType, _) => new TestRegistration());

        // Act
        var subscription = await leaseClient.SubscribeAsync("lease://acme/renderers/*", (_, _) => ValueTask.CompletedTask);

        // Assert
        Assert.Equal(MessageTypes.LeaseSubscribe, seenMessageType);
        Assert.NotNull(seenPayload);
        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("lease://acme/renderers/*", reader.ReadString());
        Assert.True(reader.IsEof);

        await subscription.DisposeAsync();
    }

    [Theory]
    [InlineData("lease://acme/renderers/**")]
    [InlineData("lease://acme/*/doc-1")]
    [InlineData("lease://*/*/*")]
    [InlineData("lease://**")]
    public async Task ShouldAcceptFullWildcardMatrixGivenValidPatternsWhenSubscribing(string pattern)
    {
        // Arrange
        using var leaseClient = new LeaseClient(
            (messageType, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                if (messageType == MessageTypes.LeaseSubscribe)
                {
                    writer.WriteU64(555);
                }
                return Task.FromResult(writer.Build());
            },
            (_, _) => new TestRegistration());

        // Act
        var subscription = await leaseClient.SubscribeAsync(pattern, (_, _) => ValueTask.CompletedTask);

        // Assert
        Assert.Equal(pattern, subscription.Route);
        await subscription.DisposeAsync();
    }

    [Theory]
    [InlineData("lease://acme/renderers/lock*")]
    [InlineData("lease://acme/*")]
    [InlineData("notlease://acme/renderers/*")]
    [InlineData("lease://acme//doc-1")]
    public async Task ShouldRejectMalformedWildcardRouteGivenInvalidPatternWhenSubscribing(string route)
    {
        // Arrange
        using var leaseClient = new LeaseClient(
            (messageType, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                if (messageType == MessageTypes.LeaseSubscribe)
                {
                    writer.WriteU64(555);
                }
                return Task.FromResult(writer.Build());
            },
            (_, _) => new TestRegistration());

        // Act
        var act = () => leaseClient.SubscribeAsync(route, (_, _) => ValueTask.CompletedTask);

        // Assert
        var error = await Assert.ThrowsAsync<LeaseException>(act);
        Assert.Equal("INVALID_ROUTE", error.Code);
    }

    [Fact]
    public async Task ShouldRestoreLeaseSubscriptionAfterReconnectGivenLeaseClientWhenOperationRuns()
    {
        // Arrange
        await using var firstTransport = new TestQueuedTransport();
        await using var secondTransport = new TestQueuedTransport();
        var firstNotification = new TaskCompletionSource<LeaseChangeEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondNotification = new TaskCompletionSource<LeaseChangeEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var notificationCount = 0;

        firstTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 1)
            {
                using var authProbeWriter = new BinaryBufferWriter();
                authProbeWriter.WriteU8(0);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, authProbeWriter.WrittenSpan));
            }
            else if (sentFrameCount == 2)
            {
                using var subscribeWriter = new BinaryBufferWriter();
                subscribeWriter.WriteU8(0);
                subscribeWriter.WriteU64(555);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseSubscribe, subscribeWriter.WrittenSpan));
            }
        };

        secondTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 1)
            {
                using var authProbeWriter = new BinaryBufferWriter();
                authProbeWriter.WriteU8(0);
                secondTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, authProbeWriter.WrittenSpan));
            }
            else if (sentFrameCount == 2)
            {
                using var subscribeWriter = new BinaryBufferWriter();
                subscribeWriter.WriteU8(0);
                subscribeWriter.WriteU64(777);
                secondTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseSubscribe, subscribeWriter.WrittenSpan));

                _ = Task.Run(async () =>
                {
                    await Task.Delay(50);
                    using var notification = new BinaryBufferWriter();
                    notification.WriteU64(777);
                    notification.WriteString("lease://prod/app/lock");
                    notification.WriteU32(0);
                    secondTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseNotify, notification.WrittenSpan));
                });
            }
        };

        var transportFactoryCalls = 0;
        Func<ITransport> transportFactory = () => transportFactoryCalls++ == 0 ? firstTransport : secondTransport;
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(10), MaxBackoff: TimeSpan.FromMilliseconds(10))),
            transportFactory);
        using var leaseClient = new LeaseClient(connection);

        await connection.ConnectAsync();
        var subscription = await leaseClient.SubscribeAsync("lease://prod/app/lock", (evt, _) =>
        {
            var seen = Interlocked.Increment(ref notificationCount);
            if (seen == 1)
            {
                firstNotification.TrySetResult(evt);
            }
            else if (seen == 2)
            {
                secondNotification.TrySetResult(evt);
            }

            return ValueTask.CompletedTask;
        });

        using (var notification = new BinaryBufferWriter())
        {
            notification.WriteU64(555);
            notification.WriteString("lease://prod/app/lock");
            notification.WriteU32(0);
            firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseNotify, notification.WrittenSpan));
        }

        // Act
        var initialEvent = await firstNotification.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.Equal("lease://prod/app/lock", initialEvent.Route);

        firstTransport.QueueClosed();

        var restoredEvent = await secondNotification.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal("lease://prod/app/lock", restoredEvent.Route);

        await connection.CloseAsync();
    }
}
