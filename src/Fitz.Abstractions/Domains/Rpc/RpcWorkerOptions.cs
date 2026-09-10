namespace Cntryl.Fitz.Abstractions.Domains.Rpc;

public sealed class RpcWorkerOptions
{
    public uint MaxConcurrency { get; init; } = 1;
}
