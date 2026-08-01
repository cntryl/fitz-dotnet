namespace Cntryl.Fitz.Errors;

public sealed class RpcException : FitzException
{
    public RpcException()
        : this("An RPC operation failed.")
    {
    }

    public RpcException(string message)
        : this(message, "UNKNOWN")
    {
    }

    public RpcException(string message, Exception innerException)
        : base(message, innerException)
    {
        Code = "UNKNOWN";
    }

    public RpcException(string message, string code, byte? status = null, uint? domainCode = null)
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
