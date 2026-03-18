using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz;

public sealed record ClientConfig(
    string Url,
    string Transport = "ws",
    TimeSpan? Timeout = null,
    TimeSpan? AuthSettleDelay = null,
    Func<CancellationToken, ValueTask<string>>? TokenProvider = null,
    Func<ClientConfig, ITransport>? TransportFactory = null
);