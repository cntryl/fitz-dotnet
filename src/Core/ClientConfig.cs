using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz;

public sealed record ClientConfig(
    string Url,
    string Transport = "ws",
    TimeSpan? Timeout = null,
    TimeSpan? AuthSettleDelay = null,
    Func<CancellationToken, ValueTask<string>>? TokenProvider = null,
    ReconnectOptions? Reconnect = null,
    int MaxFrameSize = 64 * 1024,
    Func<ClientConfig, ITransport>? TransportFactory = null
);
