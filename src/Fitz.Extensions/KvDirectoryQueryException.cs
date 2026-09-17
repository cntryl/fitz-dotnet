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
}

/// <summary>An invalid directory query that applications can map to their validation boundary.</summary>
public sealed class KvDirectoryQueryException : ArgumentException
{
    /// <summary>Creates a general invalid-cursor failure.</summary>
    public KvDirectoryQueryException()
        : this(KvDirectoryQueryError.InvalidCursor, "The directory query is invalid.")
    {
    }

    /// <summary>Creates a general invalid-cursor failure with a message.</summary>
    public KvDirectoryQueryException(string message)
        : this(KvDirectoryQueryError.InvalidCursor, message)
    {
    }

    /// <summary>Creates a general invalid-cursor failure with a message and cause.</summary>
    public KvDirectoryQueryException(string message, Exception innerException)
        : this(KvDirectoryQueryError.InvalidCursor, message, innerException)
    {
    }

    internal KvDirectoryQueryException(KvDirectoryQueryError kind, string message, Exception? inner = null)
        : base(message, inner)
    {
        Kind = kind;
    }

    /// <summary>Gets the stable failure classification.</summary>
    public KvDirectoryQueryError Kind { get; }
}
