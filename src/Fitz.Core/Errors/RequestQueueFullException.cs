namespace Cntryl.Fitz.Errors;

/// <summary>
/// Reports that the client-side request queue reached <c>ClientConfig.MaxRequestQueueSize</c>.
/// </summary>
public sealed class RequestQueueFullException : FitzException
{
    /// <summary>Initializes the exception with a message.</summary>
    /// <param name="message">Description of the failure.</param>
    public RequestQueueFullException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception with a message and underlying cause.</summary>
    /// <param name="message">Description of the failure.</param>
    /// <param name="innerException">The underlying cause.</param>
    public RequestQueueFullException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception with a default message.</summary>
    public RequestQueueFullException()
        : this("The Fitz request queue is full.")
    {
    }
}
