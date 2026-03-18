namespace Cntryl.Fitz.Errors;

public sealed class RequestTimeoutException : FitzException
{
    public RequestTimeoutException(string message)
        : base(message)
    {
    }
}