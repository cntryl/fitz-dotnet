namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public sealed class LeaseExecutionOptions
{
    public bool WaitForAvailability { get; init; }
    public uint WaitSeconds { get; init; } = 30;
}
