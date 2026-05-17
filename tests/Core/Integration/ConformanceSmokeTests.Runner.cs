namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed partial class ConformanceSmokeTests
{
    internal static async Task<AggregateResult> RunConformanceSuiteAsync()
    {
        return await RunConformanceSuiteAsync(IntegrationFixture.GetConformanceRunConfig()).ConfigureAwait(false);
    }

    internal static async Task<AggregateResult> RunConformanceSuiteAsync(ConformanceRunConfig config)
    {
        var suite = ConformanceSuiteDefinition.Load(config.SuitePath);
        var runStartedAt = DateTimeOffset.UtcNow;
        var scenarios = new List<ScenarioResult>(suite.ScenarioIds.Count);

        foreach (var scenarioId in suite.ScenarioIds)
        {
            scenarios.Add(await RunScenarioAsync(scenarioId, config.Transport, config.AuthMode).ConfigureAwait(false));
        }

        var runFinishedAt = DateTimeOffset.UtcNow;
        var aggregate = ConformanceResultBuilder.BuildAggregate(
            config.ClientName,
            suite.SuiteVersion,
            config.Transport,
            config.AuthMode,
            runStartedAt,
            runFinishedAt,
            scenarios);

        await IntegrationFixture.WriteAggregateAsync(aggregate, config.OutputPath).ConfigureAwait(false);
        return aggregate;
    }

    private static async Task<ScenarioResult> RunScenarioAsync(string scenarioId, string transport, string authMode)
    {
        return scenarioId switch
        {
            "CS-001" => await RunCs001ConnectSuccess(transport, authMode).ConfigureAwait(false),
            "CS-002" => await RunCs002AuthFailure(transport).ConfigureAwait(false),
            "CS-003" => await RunCs003RequestSuccess(transport, authMode).ConfigureAwait(false),
            "CS-004" => await RunCs004UnknownRoute(transport, authMode).ConfigureAwait(false),
            "CS-005" => await RunCs005InvalidPayload(transport, authMode).ConfigureAwait(false),
            "CS-006" => await RunCs006ServerErrorMapping(transport, authMode).ConfigureAwait(false),
            "CS-007" => await RunCs007TimeoutHandling(transport, authMode).ConfigureAwait(false),
            "CS-008" => await RunCs008CallerCancellation(transport, authMode).ConfigureAwait(false),
            "CS-009" => await RunCs009DisconnectDuringRequest(transport, authMode).ConfigureAwait(false),
            "CS-010" => await RunCs010ReconnectAndRetryBehavior(transport, authMode).ConfigureAwait(false),
            "CS-011" => await RunCs011StreamReceiveSequence(transport, authMode).ConfigureAwait(false),
            "CS-012" => await RunCs012StreamCompletion(transport, authMode).ConfigureAwait(false),
            "CS-013" => await RunCs013StreamErrorMidFlight(transport, authMode).ConfigureAwait(false),
            "CS-014" => await RunCs014ConcurrentInflightRequests(transport, authMode).ConfigureAwait(false),
            "CS-015" => await RunCs015ShutdownDuringActiveWork(transport, authMode).ConfigureAwait(false),
            "CS-017" => await RunCs017BoundedConcurrencyUnderBurstLoad(transport, authMode).ConfigureAwait(false),
            "CS-016" => await RunCs016FilteredStreamReplay(transport, authMode).ConfigureAwait(false),
            _ => throw new InvalidOperationException($"Unknown conformance scenario '{scenarioId}'."),
        };
    }

    private sealed record ConformanceSuiteDefinition(string SuiteVersion, IReadOnlyList<string> ScenarioIds)
    {
        internal static ConformanceSuiteDefinition Load(string suitePath)
        {
            if (string.IsNullOrWhiteSpace(suitePath))
            {
                throw new ArgumentException("Suite path must be provided.", nameof(suitePath));
            }

            if (!File.Exists(suitePath))
            {
                throw new FileNotFoundException($"Conformance suite file '{suitePath}' was not found.", suitePath);
            }

            var suiteVersion = "1.0";
            var scenarioIds = new List<string>();

            foreach (var line in File.ReadLines(suitePath))
            {
                var trimmed = line.Trim();

                if (trimmed.StartsWith("version:", StringComparison.OrdinalIgnoreCase))
                {
                    var versionValue = trimmed["version:".Length..].Trim().Trim('"');
                    if (!string.IsNullOrWhiteSpace(versionValue))
                    {
                        suiteVersion = versionValue;
                    }

                    continue;
                }

                if (!trimmed.StartsWith("- id:", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var scenarioId = trimmed["- id:".Length..].Trim().Trim('"');
                if (!string.IsNullOrWhiteSpace(scenarioId))
                {
                    scenarioIds.Add(scenarioId);
                }
            }

            if (scenarioIds.Count == 0)
            {
                throw new InvalidOperationException($"Conformance suite '{suitePath}' did not contain any scenario identifiers.");
            }

            return new ConformanceSuiteDefinition(suiteVersion, scenarioIds);
        }
    }
}