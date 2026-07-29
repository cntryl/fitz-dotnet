namespace Cntryl.Fitz.Errors;

public sealed class LeaseException : FitzException
{
    public LeaseException()
        : this("A lease operation failed.")
    {
    }

    public LeaseException(string message)
        : this(message, "UNKNOWN")
    {
    }

    public LeaseException(string message, Exception innerException)
        : base(message, innerException)
    {
        Code = "UNKNOWN";
    }

    public LeaseException(string message, string code, byte? status = null)
        : base(message)
    {
        Code = code;
        Status = status;
    }

    public LeaseException(string message, string code, Exception innerException)
        : base(message, innerException)
    {
        Code = code;
    }

    public string Code { get; }

    public byte? Status { get; }
}
