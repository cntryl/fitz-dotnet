using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

static class TestErrorEnvelope
{
    public static void Write(BinaryBufferWriter writer, uint code)
    {
        var category = code % 1000;
        writer.WriteU16(checked((ushort)code));
        writer.WriteU8(category <= 10 ? (byte)category : (byte)0);
        writer.WriteU8(category is 6 or 7 ? (byte)1 : (byte)0);
    }
}
