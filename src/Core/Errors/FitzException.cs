namespace Cntryl.Fitz.Errors;

public class FitzException : Exception
{
    public FitzException(string message)
        : base(message)
    {
    }

    public FitzException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}