using Cntryl.Fitz.Abstractions;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class LeaseListTests
{
    [Fact]
    public async Task should_encode_pattern_and_default_limit_given_no_cursor_when_listing()
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
            writer.WriteU32(0);
            writer.WriteU8(0);
            return Task.FromResult(writer.Build());
        });

        // Act
        var result = await leaseClient.ListAsync("lease://acme/renderers/*");

        // Assert
        Assert.Equal(MessageTypes.LeaseList, seenMessageType);
        Assert.NotNull(seenPayload);
        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("lease://acme/renderers/*", reader.ReadString());
        Assert.Equal(0, reader.ReadU8());
        Assert.Equal((uint)0, reader.ReadU32());
        Assert.True(reader.IsEof);
        Assert.Empty(result.Items);
        Assert.Null(result.NextCursor);
    }

    [Fact]
    public async Task should_encode_cursor_and_limit_when_listing()
    {
        // Arrange
        byte[]? seenPayload = null;

        using var leaseClient = new LeaseClient((_, payload, _) =>
        {
            seenPayload = payload;
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU32(0);
            writer.WriteU8(0);
            return Task.FromResult(writer.Build());
        });

        // Act
        await leaseClient.ListAsync("lease://acme/**", new LeaseListCursor(42, 100), limit: 250);

        // Assert
        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("lease://acme/**", reader.ReadString());
        Assert.Equal(1, reader.ReadU8());
        Assert.Equal((ulong)42, reader.ReadU64());
        Assert.Equal((uint)100, reader.ReadU32());
        Assert.Equal((uint)250, reader.ReadU32());
        Assert.True(reader.IsEof);
    }

    [Fact]
    public async Task should_decode_items_and_next_cursor_given_success_response_when_listing()
    {
        // Arrange
        using var leaseClient = new LeaseClient((_, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU32(2);

            writer.WriteString("lease://acme/renderers/a");
            writer.WriteString("worker-1");
            writer.WriteU64(11);
            writer.WriteString("2026-08-29T00:00:00Z");
            writer.WriteU64(30);
            writer.WriteU32(2);

            writer.WriteString("lease://acme/renderers/b");
            writer.WriteString("worker-2");
            writer.WriteU64(22);
            writer.WriteString("2026-08-29T00:01:00Z");
            writer.WriteU64(45);
            writer.WriteU32(0);

            writer.WriteU8(1);
            writer.WriteU64(999);
            writer.WriteU32(2);
            return Task.FromResult(writer.Build());
        });

        // Act
        var result = await leaseClient.ListAsync("lease://acme/renderers/*");

        // Assert
        Assert.Equal(2, result.Items.Count);
        Assert.Equal("lease://acme/renderers/a", result.Items[0].Route);
        Assert.Equal("worker-1", result.Items[0].OwnerId);
        Assert.Equal((ulong)11, result.Items[0].HolderIncarnation);
        Assert.Equal("2026-08-29T00:00:00Z", result.Items[0].AcquiredAt);
        Assert.Equal((ulong)30, result.Items[0].ExpiresInSecs);
        Assert.Equal((uint)2, result.Items[0].Renewals);

        Assert.Equal("lease://acme/renderers/b", result.Items[1].Route);
        Assert.Equal((uint)0, result.Items[1].Renewals);

        Assert.NotNull(result.NextCursor);
        Assert.Equal((ulong)999, result.NextCursor!.SnapshotId);
        Assert.Equal((uint)2, result.NextCursor.Offset);
    }

    [Fact]
    public async Task should_reject_impossible_item_count_without_unbounded_preallocation()
    {
        // Arrange
        using var leaseClient = new LeaseClient((_, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU32(uint.MaxValue);
            writer.WriteU8(0);
            return Task.FromResult(writer.Build());
        });

        // Act
        var act = () => leaseClient.ListAsync("lease://acme/renderers/*");

        // Assert
        await Assert.ThrowsAsync<InvalidOperationException>(act);
    }

    [Theory]
    [InlineData((uint)5011)]
    [InlineData((uint)5012)]
    public async Task should_preserve_domain_code_given_typed_list_error(uint domainCode)
    {
        // Arrange
        using var leaseClient = new LeaseClient((_, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(1);
            writer.WriteU32(domainCode);
            writer.WriteString("invalid list request");
            return Task.FromResult(writer.Build());
        });

        // Act
        var act = () => leaseClient.ListAsync("lease://acme/renderers/*");

        // Assert
        var error = await Assert.ThrowsAsync<LeaseException>(act);
        Assert.Equal(domainCode, error.DomainCode);
    }

    [Fact]
    public async Task should_reject_malformed_list_pattern()
    {
        // Arrange
        using var leaseClient = new LeaseClient((_, _, _) => throw new InvalidOperationException("should not send request for invalid pattern"));

        // Act
        var act = () => leaseClient.ListAsync("lease://acme/renderers/lock*");

        // Assert
        var error = await Assert.ThrowsAsync<LeaseException>(act);
        Assert.Equal("INVALID_ROUTE", error.Code);
    }

    [Fact]
    public async Task should_reject_negative_limit()
    {
        // Arrange
        using var leaseClient = new LeaseClient((_, _, _) => throw new InvalidOperationException("should not send request for invalid limit"));

        // Act
        var act = () => leaseClient.ListAsync("lease://acme/renderers/*", limit: -1);

        // Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(act);
    }

    [Fact]
    public void error_codes_match_wire_protocol()
    {
        Assert.Equal((uint)5011, FitzErrorCodes.LeaseInvalidListCursor);
        Assert.Equal((uint)5012, FitzErrorCodes.LeaseInvalidListPattern);
    }
}
