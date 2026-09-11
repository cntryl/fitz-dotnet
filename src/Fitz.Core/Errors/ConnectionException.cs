namespace Cntryl.Fitz.Errors;

/// <summary>
/// Reports that the connection is unusable or was lost. Retryable.
/// </summary>
public sealed class ConnectionException : FitzException
{
    /// <summary>Initializes the exception with a message.</summary>
    /// <param name="message">Description of the failure.</param>
    public ConnectionException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception with a message and underlying cause.</summary>
    /// <param name="message">Description of the failure.</param>
    /// <param name="innerException">The underlying cause.</param>
    public ConnectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception with a default message.</summary>
    public ConnectionException()
    {
    }
}
