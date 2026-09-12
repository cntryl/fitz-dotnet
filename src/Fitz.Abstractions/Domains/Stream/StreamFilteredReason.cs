namespace Cntryl.Fitz;

/// <summary>
/// Why the broker withheld a record or range from a read result.
/// </summary>
public enum StreamFilteredReason
{
    /// <summary>
    /// The broker withheld the content without naming a reason.
    /// </summary>
    /// <remarks>
    /// Distinct from a null <see cref="StreamReadItem.Reason"/>, which means the entry is not
    /// a filtered one at all and so has no reason to carry.
    /// </remarks>
    None = 0,

    /// <summary>Excluded by the filter supplied with the read.</summary>
    ServerFilter = 1,

    /// <summary>Withheld because the session is not permitted to see it.</summary>
    Permission = 2,

    /// <summary>Withheld by a server-side projection.</summary>
    Projection = 3,
}
