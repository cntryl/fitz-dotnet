using System.Text.Json;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Stream;

internal static class StreamWireHelpers
{
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

    internal static StreamRecord ReadRecord(ReadOnlyMemory<byte> payload, string operation)
    {
        var reader = new BinaryBufferReader(payload);
        var record = ReadRecord(reader, operation);
        if (!reader.IsEof)
        {
            throw new StreamException($"{operation} response has trailing bytes", $"{operation}_INVALID_RESPONSE");
        }

        return record;
    }

    internal static StreamRecord ReadRecord(BinaryBufferReader reader, string operation)
    {
        if (reader.RemainingBytes < 8)
        {
            throw new StreamException($"{operation} response missing record payload", $"{operation}_INVALID_RESPONSE");
        }

        var offset = reader.ReadU64();
        _ = ReadOptionalU64(reader, operation, "area offset");
        _ = ReadOptionalU64(reader, operation, "realm offset");
        var body = ReadLengthPrefixedBytes(reader, operation, "record body");
        _ = ReadOptionalBytes(reader, operation, "record metadata");

        if (reader.RemainingBytes < 8)
        {
            throw new StreamException($"{operation} response missing record timestamp", $"{operation}_INVALID_RESPONSE");
        }

        _ = reader.ReadU64();
        return new StreamRecord(offset, body);
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

    private static void ReadSuccessStatus(BinaryBufferReader reader, string operation)
    {
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new StreamException($"{operation} failed with status {status}", $"{operation}_FAILED", status);
        }
    }
}