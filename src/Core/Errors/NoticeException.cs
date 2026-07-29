namespace Cntryl.Fitz.Errors;

public sealed class NoticeException : FitzException
{
    public NoticeException()
        : this("A notice operation failed.")
    {
    }

    public NoticeException(string message)
        : this(message, "UNKNOWN")
    {
    }

    public NoticeException(string message, Exception innerException)
        : base(message, innerException)
    {
        Code = "UNKNOWN";
    }

    public NoticeException(string message, string code, byte? status = null)
        : base(message)
    {
        Code = code;
        Status = status;
    }

    public string Code { get; }

    public byte? Status { get; }
}
