namespace Cntryl.Fitz.Errors;

public sealed class KvException : FitzException
{
    public KvException()
        : this("A KV operation failed.")
    {
    }

    public KvException(string message)
        : this(message, "UNKNOWN")
    {
    }

    public KvException(string message, Exception innerException)
        : base(message, innerException)
    {
        Code = "UNKNOWN";
    }

    public KvException(string message, string code, byte? status = null)
        : base(message)
    {
        Code = code;
        Status = status;
    }

    public string Code { get; }

    public byte? Status { get; }
}
