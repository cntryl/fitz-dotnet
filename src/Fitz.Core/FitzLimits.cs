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
    /// Smallest accepted <see cref="ClientConfig.MaxFrameSize"/>: the fixed 15-byte Wire-2 header.
    /// </summary>
    public const int MinFrameSize = FrameCodec.MaxHeaderSize;

    /// <summary>
    /// Largest accepted <see cref="ClientConfig.MaxFrameSize"/>, and its default: 1 MiB including
    /// the fixed header.
    /// </summary>
    public const int MaxFrameSize = FrameCodec.MaxTransportFrameSize;
}
