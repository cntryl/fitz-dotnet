namespace Cntryl.Fitz.Abstractions.Domains.Kv;

public interface IKvClient
{
    Task<IKvTransaction> BeginAsync(
        string route,
        KvMode mode = KvMode.ReadWrite,
        KvDurability durability = KvDurability.Async,
        CancellationToken cancellationToken = default
    );
}