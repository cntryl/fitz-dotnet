using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class LeaseClientTests
{
    [Fact]
    public async Task should_return_lease_handle_given_success_response_when_acquiring_lease()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteU64(77);
            return Task.FromResult(writer.Build());
        });

        // Act
        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        // Assert
        Assert.Equal(MessageTypes.LeaseAcquire, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal((ulong)77, lease.Token);
        Assert.Equal("lease://prod/app/lock", lease.Route);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("lease://prod/app/lock", reader.ReadString());
        Assert.Equal(string.Empty, reader.ReadString());
        Assert.Equal((ulong)30, reader.ReadU64());
    }

    [Fact]
    public async Task should_return_held_lease_info_given_holder_present_when_querying_lease()
    {
        // Arrange
        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.LeaseQuery, messageType);

            var request = new BinaryBufferReader(payload);
            Assert.Equal("lease://prod/app/lock", request.ReadString());

            var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteString("worker-1");
            writer.WriteU64(18);
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
    public async Task should_encode_ttl_given_lease_handle_when_extending_lease()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();

        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            var writer = new BinaryBufferWriter();
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
    public async Task should_encode_ttl_given_lease_handle_when_renewing_lease()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();

        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            var writer = new BinaryBufferWriter();
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
        await lease.RenewAsync(90);

        // Assert
        Assert.Equal(2, calls.Count);
        Assert.Equal(MessageTypes.LeaseAcquire, calls[0].MessageType);
        Assert.Equal(MessageTypes.LeaseRenew, calls[1].MessageType);

        var renewReader = new BinaryBufferReader(calls[1].Payload);
        Assert.Equal("lease://prod/app/lock", renewReader.ReadString());
        Assert.Equal(string.Empty, renewReader.ReadString());
        Assert.Equal((ulong)77, renewReader.ReadU64());
        Assert.Equal((ulong)90, renewReader.ReadU64());
    }

    [Fact]
    public async Task should_encode_token_given_lease_handle_when_releasing_lease()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();

        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            var writer = new BinaryBufferWriter();
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
}