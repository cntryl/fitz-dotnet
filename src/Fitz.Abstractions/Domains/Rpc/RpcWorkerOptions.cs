namespace Cntryl.Fitz.Abstractions.Domains.Rpc;

/// <summary>
/// Options for an RPC worker registration.
/// </summary>
public sealed class RpcWorkerOptions
{
    /// <summary>
    /// Maximum invocations the worker handles concurrently. Defaults to one, which
    /// processes requests strictly in order. Raising it lets the broker dispatch more
    /// invocations before the handler returns.
    /// </summary>
    public uint MaxConcurrency { get; init; } = 1;
}
