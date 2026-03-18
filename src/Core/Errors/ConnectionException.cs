namespace Cntryl.Fitz.Errors;

public sealed class ConnectionException : FitzException
{
    public ConnectionException(string message)
        : base(message)
    {
    }
}