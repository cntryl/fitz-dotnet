namespace Cntryl.Fitz.Abstractions.Domains.Stream;

/// <summary>
/// Why the broker withheld a record or range from a read result.
/// </summary>
/// <remarks>Zero is not a valid reason in the Fitz protocol, so this enum has no zero member.</remarks>
public enum StreamFilteredReason
{
    /// <summary>Excluded by the filter supplied with the read.</summary>
    ServerFilter = 1,

    /// <summary>Withheld because the session is not permitted to see it.</summary>
    Permission = 2,

    /// <summary>Withheld by a server-side projection.</summary>
    Projection = 3,
}
