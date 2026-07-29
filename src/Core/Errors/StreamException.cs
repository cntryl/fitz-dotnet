namespace Cntryl.Fitz.Errors;

public sealed class StreamException : FitzException
{
    public StreamException()
        : this("A stream operation failed.")
    {
    }

    public StreamException(string message)
        : this(message, "UNKNOWN")
    {
    }

    public StreamException(string message, Exception innerException)
        : base(message, innerException)
    {
        Code = "UNKNOWN";
    }

    public StreamException(string message, string code, byte? status = null)
        : base(message)
    {
        Code = code;
        Status = status;
    }

    public string Code { get; }

    public byte? Status { get; }
}
