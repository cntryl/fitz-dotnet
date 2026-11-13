namespace Cntryl.Fitz;

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

    /// <summary>Initializes the exception for a broker CONNECT_REJECTED.</summary>
    /// <param name="message">Description of the failure.</param>
    /// <param name="rejectionCode">Broker CONNECT_REJECTED code, such as 0001 UNAUTHORIZED.</param>
    public AuthenticationException(string message, ushort rejectionCode)
        : base(message)
    {
        RejectionCode = rejectionCode;
    }

    /// <summary>The broker's CONNECT_REJECTED code, when the broker sent one.</summary>
    public ushort? RejectionCode { get; }
}
