namespace Cntryl.Fitz.Errors;

/// <summary>
/// Represents malformed or unsupported Fitz wire data.
/// </summary>
public sealed class ProtocolException : FitzException
{
    public ProtocolException()
        : this("The Fitz protocol payload is invalid.")
    {
    }

    public ProtocolException(string message)
        : base(message)
    {
    }

    public ProtocolException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
