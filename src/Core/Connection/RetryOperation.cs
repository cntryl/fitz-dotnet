namespace Cntryl.Fitz.Connection;

internal enum RetryClass
{
    WaitOnly,
    ReplayableRead,
    ConfirmedNegativeRetry,
}

internal sealed record RetryOperation(
    string Domain,
    string Operation,
    RetryClass RetryClass
);
