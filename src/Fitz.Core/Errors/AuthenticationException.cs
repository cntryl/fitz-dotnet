namespace Cntryl.Fitz.Errors;

/// <summary>
/// Reports that the broker rejected the session credentials. Authoritative and not retried.
/// </summary>
public sealed class AuthenticationException : FitzException
{
    /// <summary>Initializes the exception with a message.</summary>
    /// <param name="message">Description of the failure.</param>
    public AuthenticationException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception with a message and underlying cause.</summary>
    /// <param name="message">Description of the failure.</param>
    /// <param name="innerException">The underlying cause.</param>
    public AuthenticationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception with a default message.</summary>
    public AuthenticationException()
    {
    }
}
