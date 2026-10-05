namespace Cntryl.Fitz.Core.Tests.Integration;

/// <summary>
/// Broker restart scenarios must finish before other integration classes use the same endpoints.
/// </summary>
[CollectionDefinition("Broker restart", DisableParallelization = true)]
public sealed class BrokerRestartDefinition;
