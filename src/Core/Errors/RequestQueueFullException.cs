namespace Cntryl.Fitz.Errors;

public sealed class RequestQueueFullException : FitzException
{
    public RequestQueueFullException(string message = "The Fitz request queue is full.")
        : base(message)
    {
    }
}
