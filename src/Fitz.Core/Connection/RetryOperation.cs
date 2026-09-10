namespace Cntryl.Fitz.Connection;

enum RetryClass
{
    WaitOnly,
    ReplayableRead,
    ConfirmedNegativeRetry,
}

sealed record RetryOperation(
    string Domain,
    string Operation,
    RetryClass RetryClass
);
