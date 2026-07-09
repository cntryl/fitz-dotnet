namespace Cntryl.Fitz.Errors;

public sealed class AuthenticationException : FitzException
{
    public AuthenticationException(string message)
        : base(message)
    {
    }

    public AuthenticationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
