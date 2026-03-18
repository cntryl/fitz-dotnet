namespace Cntryl.Fitz.Errors;

public sealed class LeaseException : FitzException
{
    public LeaseException(string message, string code, byte? status = null)
        : base(message)
    {
        Code = code;
        Status = status;
    }

    public string Code { get; }

    public byte? Status { get; }
}