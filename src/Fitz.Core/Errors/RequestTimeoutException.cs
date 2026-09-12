namespace Cntryl.Fitz;

/// <summary>
/// Reports that a request exceeded its deadline before the broker answered. Retryable.
/// </summary>
public sealed class RequestTimeoutException : FitzException
{
    /// <summary>Initializes the exception with a message.</summary>
    /// <param name="message">Description of the failure.</param>
    public RequestTimeoutException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception with a message and underlying cause.</summary>
    /// <param name="message">Description of the failure.</param>
    /// <param name="innerException">The underlying cause.</param>
    public RequestTimeoutException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception with a default message.</summary>
    public RequestTimeoutException()
        : this("The Fitz request timed out.")
    {
    }
}
