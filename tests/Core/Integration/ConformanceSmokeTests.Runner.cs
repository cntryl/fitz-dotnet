namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed partial class ConformanceSmokeTests
{
    internal static readonly IReadOnlyList<string> ImplementedScenarioIds =
    [
        "CS-001",
        "CS-002",
        "CS-003",
        "CS-004",
        "CS-005",
        "CS-006",
        "CS-007",
        "CS-008",
        "CS-009",
        "CS-010",
        "CS-011",
        "CS-012",
        "CS-013",
        "CS-014",
        "CS-015",
        "CS-016",
        "CS-017",
    ];

    internal static async Task<AggregateResult> RunConformanceSuiteAsync()
    {
        return await RunConformanceSuiteAsync(IntegrationFixture.GetConformanceRunConfig()).ConfigureAwait(false);
    }

    internal static async Task<AggregateResult> RunConformanceSuiteAsync(ConformanceRunConfig config)
    {
        var suite = ConformanceSuiteDefinition.Load(config.SuitePath);
        var scenarios = new List<ScenarioResult>(suite.Scenarios.Count);

        foreach (var scenario in suite.Scenarios)
        {
            scenarios.Add(await RunScenarioAsync(scenario.ScenarioId, config.Transport, config.AuthMode).ConfigureAwait(false));
        }

        var aggregate = ConformanceResultBuilder.BuildAggregate(
            suite.Name,
            config.ClientName,
            suite.SuiteVersion,
            config.Transport,
            config.AuthMode,
            DateTimeOffset.UtcNow,
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

    internal sealed record ConformanceSuiteDefinition(
        string Name,
        string SuiteVersion,
        IReadOnlyList<ConformanceScenarioDefinition> Scenarios)
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
            var suiteName = "fitz-cross-language-client-conformance";
            var scenarios = new List<ConformanceScenarioDefinition>();
            string? pendingId = null;
            string? pendingTitle = null;
            string? pendingPriority = null;

            foreach (var line in File.ReadLines(suitePath))
            {
                var trimmed = line.Trim();

                if (trimmed.StartsWith("name:", StringComparison.OrdinalIgnoreCase))
                {
                    var nameValue = trimmed["name:".Length..].Trim().Trim('"');
                    if (!string.IsNullOrWhiteSpace(nameValue))
                    {
                        suiteName = nameValue;
                    }

                    continue;
                }

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
                    if (pendingId is not null && trimmed.StartsWith("title:", StringComparison.OrdinalIgnoreCase))
                    {
                        pendingTitle = trimmed["title:".Length..].Trim().Trim('"');
                    }
                    else if (pendingId is not null && trimmed.StartsWith("priority:", StringComparison.OrdinalIgnoreCase))
                    {
                        pendingPriority = trimmed["priority:".Length..].Trim().Trim('"');
                    }

                    continue;
                }

                if (!string.IsNullOrWhiteSpace(pendingId))
                {
                    scenarios.Add(new ConformanceScenarioDefinition(
                        pendingId,
                        pendingTitle ?? pendingId,
                        pendingPriority ?? "P1"));
                }

                pendingId = trimmed["- id:".Length..].Trim().Trim('"');
                pendingTitle = null;
                pendingPriority = null;
            }

            if (!string.IsNullOrWhiteSpace(pendingId))
            {
                scenarios.Add(new ConformanceScenarioDefinition(
                    pendingId,
                    pendingTitle ?? pendingId,
                    pendingPriority ?? "P1"));
            }

            if (scenarios.Count == 0)
            {
                throw new InvalidOperationException($"Conformance suite '{suitePath}' did not contain any scenario identifiers.");
            }

            return new ConformanceSuiteDefinition(suiteName, suiteVersion, scenarios);
        }
    }

    internal sealed record ConformanceScenarioDefinition(string ScenarioId, string Title, string Priority);
}
