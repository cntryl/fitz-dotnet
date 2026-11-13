namespace Cntryl.Fitz.Extensions;

/// <summary>Classifies caller-controlled directory query failures.</summary>
public enum KvDirectoryQueryError
{
    /// <summary>The cursor cannot be decoded safely.</summary>
    InvalidCursor,
    /// <summary>The cursor belongs to a different route or query shape.</summary>
    CursorMismatch,
    /// <summary>The requested page size is outside configured bounds.</summary>
    InvalidLimit,
    /// <summary>The requested index generation is not part of this directory schema.</summary>
    UnsupportedIndex,
    /// <summary>
    /// The failure was created without a classification, by one of the standard exception
    /// constructors. The directory itself never raises this kind.
    /// </summary>
    Unclassified,
}

/// <summary>An invalid directory query that applications can map to their validation boundary.</summary>
public sealed class KvDirectoryQueryException : ArgumentException
{
    /// <summary>Creates an unclassified failure; <see cref="Kind"/> is <see cref="KvDirectoryQueryError.Unclassified"/>.</summary>
    public KvDirectoryQueryException()
        : this(KvDirectoryQueryError.Unclassified, "The directory query is invalid.")
    {
    }

    /// <summary>Creates an unclassified failure with a message.</summary>
    /// <param name="message">Description of the failure.</param>
    public KvDirectoryQueryException(string message)
        : this(KvDirectoryQueryError.Unclassified, message)
    {
    }

    /// <summary>Creates an unclassified failure with a message and cause.</summary>
    /// <param name="message">Description of the failure.</param>
    /// <param name="innerException">The underlying cause.</param>
    public KvDirectoryQueryException(string message, Exception innerException)
        : this(KvDirectoryQueryError.Unclassified, message, null, innerException)
    {
    }

    /// <summary>Creates a classified failure.</summary>
    /// <param name="kind">The stable classification.</param>
    /// <param name="message">Description of the failure.</param>
    /// <param name="paramName">The name of the argument that caused the failure, if there is one.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public KvDirectoryQueryException(
        KvDirectoryQueryError kind,
        string message,
        string? paramName = null,
        Exception? innerException = null)
        : base(message, paramName, innerException)
    {
        Kind = kind;
    }

    /// <summary>Gets the stable failure classification.</summary>
    public KvDirectoryQueryError Kind { get; }
}
