namespace Cntryl.Fitz;

/// <summary>
/// Represents malformed or unsupported Fitz wire data.
/// </summary>
/// <summary>
/// Reports a malformed or unexpected frame on the wire.
/// </summary>
public sealed class ProtocolException : FitzException
{
    /// <summary>Initializes the exception with a default message.</summary>
    public ProtocolException()
        : this("The Fitz protocol payload is invalid.")
    {
    }

    /// <summary>Initializes the exception with a message.</summary>
    /// <param name="message">Description of the failure.</param>
    public ProtocolException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a protocol error returned during CONNECT.</summary>
    /// <param name="message">Description of the failure.</param>
    /// <param name="rejectionCode">Broker CONNECT_REJECTED code.</param>
    public ProtocolException(string message, ushort rejectionCode)
        : base(message)
    {
        RejectionCode = rejectionCode;
    }

    /// <summary>The broker's CONNECT_REJECTED code, when available.</summary>
    public ushort? RejectionCode { get; }

    /// <summary>Initializes the exception with a message and underlying cause.</summary>
    /// <param name="message">Description of the failure.</param>
    /// <param name="innerException">The underlying cause.</param>
    public ProtocolException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
