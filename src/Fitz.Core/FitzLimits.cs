using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz;

/// <summary>
/// Protocol bounds a caller needs in order to validate configuration ahead of
/// <see cref="ClientConfig.Validate"/>.
/// </summary>
/// <remarks>
/// These are fixed by the Fitz wire format, not by this client, so they are safe to compare
/// against at any time. The codec that defines them is internal; this type is the supported
/// way to read them.
/// </remarks>
public static class FitzLimits
{
    /// <summary>
    /// Smallest accepted <see cref="ClientConfig.MaxFrameSize"/>: a frame header on its own,
    /// being a 16-bit opcode plus a length prefix.
    /// </summary>
    public const int MinFrameSize = FrameCodec.MaxHeaderSize;

    /// <summary>
    /// Largest accepted <see cref="ClientConfig.MaxFrameSize"/>, and its default. The Fitz
    /// payload length is 16-bit, and a correlated transport frame additionally carries its
    /// correlation record, so the ceiling is the sum of the two plus a header.
    /// </summary>
    public const int MaxFrameSize = FrameCodec.MaxTransportFrameSize;
}
