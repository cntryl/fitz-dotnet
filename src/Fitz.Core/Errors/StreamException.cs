namespace Cntryl.Fitz.Errors;

/// <summary>
/// Reports a failed stream operation, carrying the broker's structured error code.
/// </summary>
/// <remarks>
/// Match on <see cref="DomainCode"/> against <c>FitzErrorCodes</c>, or on <see cref="Code"/>,
/// rather than on message text. Use <see cref="Retryability.IsRetryable"/> to decide whether
/// the operation may be retried.
/// </remarks>
public sealed class StreamException : FitzException
{
    /// <summary>Initializes the exception with a default message.</summary>
    public StreamException()
        : this("A stream operation failed.")
    {
    }

    /// <summary>Initializes the exception with a message and an unknown code.</summary>
    /// <param name="message">Description of the failure.</param>
    public StreamException(string message)
        : this(message, "UNKNOWN")
    {
    }

    /// <summary>Initializes the exception with a message and underlying cause.</summary>
    /// <param name="message">Description of the failure.</param>
    /// <param name="innerException">The underlying cause.</param>
    public StreamException(string message, Exception innerException)
        : base(message, innerException)
    {
        Code = "UNKNOWN";
    }

    /// <summary>Initializes the exception from a broker error response.</summary>
    /// <param name="message">Description of the failure.</param>
    /// <param name="code">Symbolic error code reported by the broker.</param>
    /// <param name="status">Wire status byte, when the response carried one.</param>
    /// <param name="domainCode">Numeric domain code, comparable against <c>FitzErrorCodes</c>.</param>
    public StreamException(string message, string code, byte? status = null, uint? domainCode = null)
        : base(message)
    {
        Code = code;
        Status = status;
        DomainCode = domainCode;
    }

    /// <summary>Symbolic error code, or <c>"UNKNOWN"</c> when the broker reported none.</summary>
    public string Code { get; }

    /// <summary>Wire status byte, when the broker response carried one.</summary>
    public byte? Status { get; }

    /// <summary>Numeric domain code, comparable against <c>FitzErrorCodes</c>.</summary>
    public uint? DomainCode { get; }
}
