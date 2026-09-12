namespace Cntryl.Fitz;

/// <summary>
/// Base class for every exception raised by the Fitz client.
/// </summary>
public class FitzException : Exception
{
    /// <summary>Initializes the exception with a message.</summary>
    /// <param name="message">Description of the failure.</param>
    public FitzException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception with a message and underlying cause.</summary>
    /// <param name="message">Description of the failure.</param>
    /// <param name="innerException">The underlying cause.</param>
    public FitzException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception with a default message.</summary>
    public FitzException()
    {
    }
}
