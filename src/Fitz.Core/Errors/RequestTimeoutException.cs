namespace Cntryl.Fitz.Errors;

public sealed class RequestTimeoutException : FitzException
{
    public RequestTimeoutException(string message)
        : base(message)
    {
    }

    public RequestTimeoutException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public RequestTimeoutException()
        : this("The Fitz request timed out.")
    {
    }
}
