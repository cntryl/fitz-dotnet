namespace Cntryl.Fitz.Errors;

public sealed class KvException : FitzException
{
    public KvException(string message, string code, byte? status = null)
        : base(message)
    {
        Code = code;
        Status = status;
    }

    public string Code { get; }

    public byte? Status { get; }
}