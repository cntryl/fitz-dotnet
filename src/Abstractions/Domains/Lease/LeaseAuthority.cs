namespace Cntryl.Fitz.Abstractions.Domains.Lease;

/// <summary>
/// Immutable admission authority captured from the successful lease acquisition.
/// </summary>
/// <remarks>
/// The fencing token identifies the ownership epoch that admitted the managed callback. It remains
/// stable for that callback even when renewal rotates the lease handle's live credential.
/// </remarks>
public readonly record struct LeaseAuthority(ulong FencingToken);
