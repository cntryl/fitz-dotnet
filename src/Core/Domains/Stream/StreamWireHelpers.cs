using System.IO;
using System.Text.Json;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Collections.Generic;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Core;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Stream;

internal static class StreamWireHelpers
{
    internal static byte[] EncodeStreamFilterSet(StreamFilterSet? filter)
    {
        if (filter is null || filter.Clauses.Count == 0)
        {
            return Array.Empty<byte>();
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteU8(0);
        writer.WriteU8(0xF1);
        writer.WriteU32((uint)filter.Clauses.Count);
        foreach (var clause in filter.Clauses)
        {
            writer.WriteU8((byte)clause.Kind);
            switch (clause.Kind)
            {
                case StreamFilterClauseKind.Equals:
                case StreamFilterClauseKind.NotEquals:
                case StreamFilterClauseKind.StartsWith:
                    WriteBincodeString(writer, clause.Value ?? string.Empty);
                    break;
                case StreamFilterClauseKind.AnyOf:
                    writer.WriteU32((uint)clause.Values.Count);
                    foreach (var value in clause.Values)
                    {
                        WriteBincodeString(writer, value);
                    }

                    break;
            }
        }

        return writer.Build();
    }

    internal static void EnsureSuccessStatusOnly(ReadOnlyMemory<byte> response, string operation)
    {
        var reader = new BinaryBufferReader(response);
        ReadSuccessStatus(reader, operation);
    }

    internal static ulong ReadExpectedSessionId(ReadOnlyMemory<byte> response, string operation, string missingSessionCode)
    {
        var reader = new BinaryBufferReader(response);
        ReadSuccessStatus(reader, operation);

        if (reader.IsEof || reader.ReadU8() != 1 || reader.RemainingBytes < 8)
        {
            throw new StreamException($"{operation} response missing session id", missingSessionCode);
        }

        var sessionId = reader.ReadU64();
        if (!reader.IsEof)
        {
            if (reader.RemainingBytes < 4)
            {
                throw new StreamException($"{operation} response has trailing bytes", $"{operation}_INVALID_RESPONSE");
            }

            var payloadLength = reader.ReadU32();
            if (payloadLength > int.MaxValue)
            {
                throw new StreamException($"{operation} response payload length too large", $"{operation}_INVALID_RESPONSE");
            }

            _ = reader.ReadMemory((int)payloadLength);
        }

        if (!reader.IsEof)
        {
            throw new StreamException($"{operation} response has trailing bytes", $"{operation}_INVALID_RESPONSE");
        }

        return sessionId;
    }

    internal static ReadOnlyMemory<byte> ReadOptionalPayload(ReadOnlyMemory<byte> response, string operation)
    {
        var reader = new BinaryBufferReader(response);
        ReadSuccessStatus(reader, operation);

        if (reader.IsEof)
        {
            return ReadOnlyMemory<byte>.Empty;
        }

        var hasSession = reader.ReadU8();
        if (hasSession != 0 && hasSession != 1)
        {
            throw new StreamException($"{operation} response has invalid session flag {hasSession}", $"{operation}_INVALID_RESPONSE");
        }

        if (hasSession == 1)
        {
            if (reader.RemainingBytes < 8)
            {
                throw new StreamException($"{operation} response missing session id", $"{operation}_INVALID_RESPONSE");
            }

            _ = reader.ReadU64();
        }

        if (reader.IsEof)
        {
            return ReadOnlyMemory<byte>.Empty;
        }

        if (reader.RemainingBytes < 4)
        {
            throw new StreamException($"{operation} response missing payload length", $"{operation}_INVALID_RESPONSE");
        }

        var payloadLength = reader.ReadU32();
        if (payloadLength > int.MaxValue)
        {
            throw new StreamException($"{operation} response payload length too large", $"{operation}_INVALID_RESPONSE");
        }

        var payload = reader.ReadMemory((int)payloadLength);
        if (!reader.IsEof)
        {
            throw new StreamException($"{operation} response has trailing bytes", $"{operation}_INVALID_RESPONSE");
        }

        return payload;
    }

    internal static StreamRecord ReadRecord(ReadOnlyMemory<byte> payload, string operation, string route)
    {
        var reader = new BinaryBufferReader(payload);
        var record = ReadRecord(reader, operation, route);
        if (!reader.IsEof)
        {
            throw new StreamException($"{operation} response has trailing bytes", $"{operation}_INVALID_RESPONSE");
        }

        return record;
    }

    internal static StreamRecord ReadRoutedRecord(ReadOnlyMemory<byte> payload, string operation)
    {
        var reader = new BinaryBufferReader(payload);
        var route = reader.ReadString();
        if (!RouteValidation.TryValidateFixedRoute(route, "stream", 3, out _))
        {
            throw new StreamException($"{operation} response contains invalid concrete stream route: {route}", $"{operation}_INVALID_RESPONSE");
        }
        var record = ReadRecord(reader, operation, route);
        if (!reader.IsEof)
        {
            throw new StreamException($"{operation} response has trailing bytes", $"{operation}_INVALID_RESPONSE");
        }
        return record;
    }

    internal static StreamRecord ReadRecord(BinaryBufferReader reader, string operation, string route)
    {
        if (reader.RemainingBytes < 8)
        {
            throw new StreamException($"{operation} response missing record payload", $"{operation}_INVALID_RESPONSE");
        }

        var offset = reader.ReadU64();
        var areaOffset = ReadOptionalU64(reader, operation, "area offset");
        var realmOffset = ReadOptionalU64(reader, operation, "realm offset");
        var body = ReadLengthPrefixedBytes(reader, operation, "record body");
        var metadata = ReadOptionalBytes(reader, operation, "record metadata")?.ToArray();

        if (reader.RemainingBytes < 8)
        {
            throw new StreamException($"{operation} response missing record timestamp", $"{operation}_INVALID_RESPONSE");
        }

        var timestamp = reader.ReadU64();
        return new StreamRecord(route, offset, areaOffset, realmOffset, body, metadata, timestamp);
    }

    internal static StreamReadPage ReadReadPage(ReadOnlyMemory<byte> payload, string operation)
    {
        var reader = new BinaryBufferReader(payload);
        if (reader.IsEof)
        {
            return new StreamReadPage(Array.Empty<StreamReadItem>(), new StreamReadCursor(0, null, null, false));
        }

        if (reader.RemainingBytes < 4)
        {
            throw new StreamException($"{operation} response missing item count", $"{operation}_INVALID_RESPONSE");
        }

        var itemCount = reader.ReadU32();
        if (itemCount > int.MaxValue)
        {
            throw new StreamException($"{operation} response item count too large", $"{operation}_INVALID_RESPONSE");
        }

        var items = new List<StreamReadItem>((int)itemCount);
        for (var index = 0; index < itemCount; index++)
        {
            var route = reader.ReadString();
            if (!RouteValidation.IsFixedRoute(route, "stream", 3))
            {
                throw new StreamException($"{operation} response contains invalid concrete stream route '{route}'", $"{operation}_INVALID_RESPONSE");
            }
            items.Add(ReadReadItem(reader, operation, route));
        }

        if (reader.RemainingBytes < 8)
        {
            throw new StreamException($"{operation} response missing read cursor", $"{operation}_INVALID_RESPONSE");
        }

        var cursor = new StreamReadCursor(
            reader.ReadU64(),
            ReadOptionalU64(reader, operation, "last area offset"),
            ReadOptionalU64(reader, operation, "last realm offset"),
            ReadBoolFlag(reader, operation, "has more flag"));

        if (!reader.IsEof)
        {
            throw new StreamException($"{operation} response had trailing data", $"{operation}_INVALID_RESPONSE");
        }

        return new StreamReadPage(items, cursor);
    }

    internal static StreamReadItem ReadReadItem(BinaryBufferReader reader, string operation, string route)
    {
        if (reader.IsEof)
        {
            throw new StreamException($"{operation} response missing read item tag", $"{operation}_INVALID_RESPONSE");
        }

        var tag = reader.ReadU8();
        return tag switch
        {
            0 => new StreamReadItem(route, StreamReadItemKind.Event, Record: ReadRecord(reader, operation, route)),
            1 => new StreamReadItem(
                route,
                StreamReadItemKind.Filtered,
                Offset: reader.ReadU64(),
                Reason: ReadFilteredReason(reader, operation)),
            2 => new StreamReadItem(
                route,
                StreamReadItemKind.FilteredRange,
                FromOffset: reader.ReadU64(),
                ToOffset: reader.ReadU64(),
                Reason: ReadFilteredReason(reader, operation)),
            _ => throw new StreamException($"{operation} response has invalid read item tag {tag}", $"{operation}_INVALID_RESPONSE"),
        };
    }

    internal static StreamFilteredReason? ReadFilteredReason(BinaryBufferReader reader, string operation)
    {
        if (reader.IsEof)
        {
            throw new StreamException($"{operation} response missing filtered reason", $"{operation}_INVALID_RESPONSE");
        }

        var tag = reader.ReadU8();
        return tag switch
        {
            0 => null,
            1 => StreamFilteredReason.ServerFilter,
            2 => StreamFilteredReason.Permission,
            3 => StreamFilteredReason.Projection,
            _ => throw new StreamException($"{operation} response has invalid filtered reason tag {tag}", $"{operation}_INVALID_RESPONSE"),
        };
    }

    internal static bool ReadBoolFlag(BinaryBufferReader reader, string operation, string fieldName)
    {
        if (reader.IsEof)
        {
            throw new StreamException($"{operation} response missing {fieldName}", $"{operation}_INVALID_RESPONSE");
        }

        var flag = reader.ReadU8();
        return flag switch
        {
            0 => false,
            1 => true,
            _ => throw new StreamException($"{operation} response has invalid {fieldName} {flag}", $"{operation}_INVALID_RESPONSE"),
        };
    }

    internal static ulong? ReadOptionalU64(BinaryBufferReader reader, string operation, string fieldName)
    {
        if (reader.IsEof)
        {
            throw new StreamException($"{operation} response missing {fieldName}", $"{operation}_INVALID_RESPONSE");
        }

        var flag = reader.ReadU8();
        return flag switch
        {
            0 => null,
            1 when reader.RemainingBytes >= 8 => reader.ReadU64(),
            1 => throw new StreamException($"{operation} response missing {fieldName}", $"{operation}_INVALID_RESPONSE"),
            _ => throw new StreamException($"{operation} response has invalid {fieldName} flag {flag}", $"{operation}_INVALID_RESPONSE"),
        };
    }

    internal static ReadOnlyMemory<byte>? ReadOptionalBytes(BinaryBufferReader reader, string operation, string fieldName)
    {
        if (reader.IsEof)
        {
            throw new StreamException($"{operation} response missing {fieldName}", $"{operation}_INVALID_RESPONSE");
        }

        var flag = reader.ReadU8();
        return flag switch
        {
            0 => null,
            1 => ReadLengthPrefixedMemory(reader, operation, fieldName),
            _ => throw new StreamException($"{operation} response has invalid {fieldName} flag {flag}", $"{operation}_INVALID_RESPONSE"),
        };
    }

    internal static byte[] ReadLengthPrefixedBytes(BinaryBufferReader reader, string operation, string fieldName)
    {
        var payload = ReadLengthPrefixedMemory(reader, operation, fieldName);
        return payload.ToArray();
    }

    internal static ReadOnlyMemory<byte> ReadLengthPrefixedMemory(BinaryBufferReader reader, string operation, string fieldName)
    {
        if (reader.RemainingBytes < 4)
        {
            throw new StreamException($"{operation} response missing {fieldName} length", $"{operation}_INVALID_RESPONSE");
        }

        var payloadLength = reader.ReadU32();
        if (payloadLength > int.MaxValue)
        {
            throw new StreamException($"{operation} response {fieldName} length too large", $"{operation}_INVALID_RESPONSE");
        }

        var payloadLengthInt = (int)payloadLength;
        if (reader.RemainingBytes < payloadLengthInt)
        {
            throw new StreamException($"{operation} response truncated {fieldName}", $"{operation}_INVALID_RESPONSE");
        }

        return reader.ReadMemory(payloadLengthInt);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Best-effort metadata parsing treats malformed broker payloads as an absent offset.")]
    internal static ulong TryParseCommitOffset(ReadOnlySpan<byte> payload)
    {
        try
        {
            var reader = new Utf8JsonReader(payload, isFinalBlock: true, state: default);
            while (reader.Read())
            {
                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    continue;
                }

                if (reader.ValueTextEquals("last_resource_offset"u8))
                {
                    return reader.Read() && reader.TokenType == JsonTokenType.Number ? reader.GetUInt64() : 0;
                }

                if (reader.ValueTextEquals("first_resource_offset"u8))
                {
                    return reader.Read() && reader.TokenType == JsonTokenType.Number ? reader.GetUInt64() : 0;
                }
            }
        }
        catch
        {
        }

        return 0;
    }

    private static void WriteBincodeString(BinaryBufferWriter writer, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        writer.WriteU32((uint)bytes.Length);
        writer.WriteBytes(bytes);
    }

    private static void ReadSuccessStatus(BinaryBufferReader reader, string operation)
    {
        var status = reader.ReadU8();
        if (status != 0)
        {
            if (status == 1)
            {
                var domainCode = reader.ReadU32();
                var message = reader.ReadString();
                if (!reader.IsEof)
                {
                    throw new StreamException($"{operation} error response has trailing bytes", $"{operation}_INVALID_RESPONSE", status, domainCode);
                }
                throw new StreamException($"{operation} failed: {message}", $"{operation}_FAILED", status, domainCode);
            }
            throw new StreamException($"{operation} failed with status {status}", $"{operation}_FAILED", status);
        }
    }
}
