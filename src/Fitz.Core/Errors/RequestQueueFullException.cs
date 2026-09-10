namespace Cntryl.Fitz.Errors;

public sealed class RequestQueueFullException : FitzException
{
    public RequestQueueFullException(string message)
        : base(message)
    {
    }

    public RequestQueueFullException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public RequestQueueFullException()
        : this("The Fitz request queue is full.")
    {
    }
}
