namespace Cntryl.Fitz.Errors;

public sealed class ScheduleException : FitzException
{
    public ScheduleException(string message, string code, byte? status = null, uint? domainCode = null)
        : base(message)
    {
        Code = code;
        Status = status;
        DomainCode = domainCode;
    }

    public string Code { get; }

    public byte? Status { get; }

    public uint? DomainCode { get; }
}
