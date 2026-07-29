namespace Cntryl.Fitz.Errors;

public sealed class QueueException : FitzException
{
    public QueueException()
        : this("A queue operation failed.")
    {
    }

    public QueueException(string message)
        : this(message, "UNKNOWN")
    {
    }

    public QueueException(string message, Exception innerException)
        : base(message, innerException)
    {
        Code = "UNKNOWN";
    }

    public QueueException(string message, string code, byte? status = null)
        : base(message)
    {
        Code = code;
        Status = status;
    }

    public string Code { get; }

    public byte? Status { get; }
}
