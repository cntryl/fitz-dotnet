using System.Collections.Immutable;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Analyzers;
using Cntryl.Fitz.CodeFixes;
using Cntryl.Fitz.Domains.Lease;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Text;

namespace Cntryl.Fitz.Analyzers.Tests;

public sealed class FitzUsageAnalyzerTests
{
    [Theory]
    [InlineData("await client.AcquireAsync(\"queue://realm/area/name\", 30);", FitzDiagnostics.InvalidRouteId)]
    [InlineData("await client.ListAsync(\"lease://realm/bad*value/name\");", FitzDiagnostics.InvalidPatternId)]
    [InlineData("await client.AcquireAsync(\"lease://realm/area/name\", 0);", FitzDiagnostics.InvalidArgumentId)]
    [InlineData("_ = await client.ListAsync(\"lease://realm/area/*\", limit: 0);", FitzDiagnostics.InvalidArgumentId)]
    public async Task ShouldReportInvalidConstantUsageGivenAnalyzerInputWhenAnalyzing(string operation, string expectedId)
    {
        // Arrange
        // Act
        // Assert
        var diagnostics = await GetDiagnosticsAsync(LeaseSource(operation));

        Assert.Contains(diagnostics, diagnostic => diagnostic.Id == expectedId);
    }

    [Fact]
    public async Task ShouldReportDiscardedLifecycleHandleGivenAnalyzerInputWhenAnalyzing()
    {
        // Arrange
        // Act
        // Assert
        var diagnostics = await GetDiagnosticsAsync(LeaseSource(
            "await client.AcquireAsync(\"lease://realm/area/name\", 30);"));

        Assert.Contains(diagnostics, diagnostic => diagnostic.Id == FitzDiagnostics.DiscardedHandleId);
    }

    [Fact]
    public async Task ShouldAcceptRetainedHandleAndNonconstantRouteGivenAnalyzerInputWhenAnalyzing()
    {
        // Arrange
        // Act
        // Assert
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz.Abstractions.Domains.Lease;
            class Consumer
            {
                static async Task Run(ILeaseClient client, string route)
                {
                    await using var lease = await client.AcquireAsync(route, 30);
                }
            }
            """);

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task ShouldReportDiagnosticsGivenConcreteClientInvocationWhenArgumentsInvalid()
    {
        // Arrange
        // Act
        // Assert
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz.Domains.Lease;
            class Consumer
            {
                static async Task Run(LeaseClient client)
                {
                    await using var lease = await client.AcquireAsync("queue://realm/area/name", 0);
                }
            }
            """);

        Assert.Contains(diagnostics, diagnostic => diagnostic.Id == FitzDiagnostics.InvalidRouteId);
        Assert.Contains(diagnostics, diagnostic => diagnostic.Id == FitzDiagnostics.InvalidArgumentId);
    }

    [Fact]
    public async Task ShouldAcceptRuntimeSupportedRealmStreamSelectorGivenAnalyzerInputWhenAnalyzing()
    {
        // Arrange
        // Act
        // Assert
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz.Abstractions.Domains.Stream;
            class Consumer
            {
                static async Task Run(IStreamClient client)
                {
                    await using var subscription = await client.SubscribeAsync("stream://realm/**");
                }
            }
            """);

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task ShouldAcceptRuntimeSupportedStreamReadSelectorsGivenAnalyzerInputWhenAnalyzing()
    {
        // Arrange
        // Act
        // Assert
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz.Abstractions.Domains.Stream;
            class Consumer
            {
                static async Task Run(IStreamClient client)
                {
                    await foreach (var record in client.ReadAsync("stream://prod/app/*", 0))
                    {
                        _ = record;
                    }

                    _ = await client.ReadPageAsync("stream://**", 0);
                }
            }
            """);

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task ShouldProduceNoRouteDiagnosticGivenLeaseWildcardPatternWhenAnalyzingSubscribe()
    {
        // Arrange
        // Act
        // Assert
        var diagnostics = await GetDiagnosticsAsync(LeaseSource(
            "await using var subscription = await client.SubscribeAsync(\"lease://prod/app/*\");"));

        Assert.DoesNotContain(diagnostics, diagnostic =>
            diagnostic.Id is FitzDiagnostics.InvalidRouteId or FitzDiagnostics.InvalidPatternId);
    }

    [Fact]
    public async Task ShouldReportNonWholeSecondQueueDelayGivenAnalyzerInputWhenAnalyzing()
    {
        // Arrange
        // Act
        // Assert
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz.Abstractions.Domains.Queue;
            class Consumer
            {
                static async Task Run(IQueueClient client)
                {
                    _ = await client.EnqueueAsync(
                        "queue://realm/area/name",
                        ReadOnlyMemory<byte>.Empty,
                        delayMs: 1);
                }
            }
            """);

        Assert.Contains(diagnostics, diagnostic => diagnostic.Id == FitzDiagnostics.InvalidArgumentId);
    }

    [Fact]
    public async Task ShouldIgnoreLookalikeConsumerInterfaceGivenAnalyzerInputWhenAnalyzing()
    {
        // Arrange
        // Act
        // Assert
        var diagnostics = await GetDiagnosticsAsync("""
            namespace Cntryl.Fitz.Consumer;
            interface ILeaseClient { Task AcquireAsync(string route, ulong ttlSecs); }
            class Example
            {
                static async Task Run(ILeaseClient client) =>
                    await client.AcquireAsync("queue://realm/area/name", 0);
            }
            """);

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task ShouldReplaceWrongSchemeGivenRouteDiagnosticWhenApplyingFix()
    {
        // Arrange
        // Act
        // Assert
        var fixedSource = await ApplyFirstFixAsync(
            LeaseSource("await using var lease = await client.AcquireAsync(\"queue://realm/area/name\", 30);"),
            FitzDiagnostics.InvalidRouteId);

        Assert.Contains("\"lease://realm/area/name\"", fixedSource, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShouldReportWarningGivenInvalidRouteWhenAnalyzing()
    {
        // Arrange
        // Act
        // Assert
        var diagnostics = await GetDiagnosticsAsync(LeaseSource(
            "await using var lease = await client.AcquireAsync(\"lease://realm/bad*value/name\", 30);"));

        var diagnostic = Assert.Single(diagnostics.Where(item => item.Id == FitzDiagnostics.InvalidRouteId));
        Assert.Equal(DiagnosticSeverity.Warning, diagnostic.Severity);
    }

    [Fact]
    public async Task ShouldNotOfferFixGivenInvalidSchemeReplacementWhenRegisteringRouteFix()
    {
        // Arrange
        using var workspace = CreateWorkspace(
            LeaseSource("await using var lease = await client.AcquireAsync(\"queue://realm\", 30);"),
            out var document);

        // Act
        var compilation = await document.Project.GetCompilationAsync();

        // Assert
        Assert.NotNull(compilation);
        var diagnostic = Assert.Single((await compilation.WithAnalyzers([new FitzUsageAnalyzer()])
            .GetAnalyzerDiagnosticsAsync()).Where(item => item.Id == FitzDiagnostics.InvalidRouteId));
        var actions = new List<Microsoft.CodeAnalysis.CodeActions.CodeAction>();
        var provider = new FitzCodeFixProvider();

        await provider.RegisterCodeFixesAsync(new CodeFixContext(
            document,
            diagnostic,
            (action, _) => actions.Add(action),
            CancellationToken.None));

        Assert.Empty(actions);
    }

    [Fact]
    public async Task ShouldRetainAndDisposeResultGivenDiscardedHandleDiagnosticWhenApplyingFix()
    {
        // Arrange
        // Act
        // Assert
        var fixedSource = await ApplyFirstFixAsync(
            LeaseSource("await client.AcquireAsync(\"lease://realm/area/name\", 30);"),
            FitzDiagnostics.DiscardedHandleId);

        Assert.Contains("await using var lease = await client.AcquireAsync", fixedSource, StringComparison.Ordinal);
    }

    static string LeaseSource(string operation) => $$"""
        using Cntryl.Fitz.Abstractions.Domains.Lease;
        class Consumer
        {
            static async Task Run(ILeaseClient client)
            {
                {{operation}}
            }
        }
        """;

    static async Task<ImmutableArray<Diagnostic>> GetDiagnosticsAsync(string source)
    {
        using var workspace = CreateWorkspace(source, out var document);
        var compilation = await document.Project.GetCompilationAsync();
        Assert.NotNull(compilation);
        Assert.DoesNotContain(compilation.GetDiagnostics(), diagnostic => diagnostic.Severity == DiagnosticSeverity.Error);
        return await compilation.WithAnalyzers([new FitzUsageAnalyzer()]).GetAnalyzerDiagnosticsAsync();
    }

    static async Task<string> ApplyFirstFixAsync(string source, string diagnosticId)
    {
        using var workspace = CreateWorkspace(source, out var document);
        var compilation = await document.Project.GetCompilationAsync();
        Assert.NotNull(compilation);
        Assert.DoesNotContain(compilation.GetDiagnostics(), diagnostic => diagnostic.Severity == DiagnosticSeverity.Error);
        var diagnostics = await compilation.WithAnalyzers([new FitzUsageAnalyzer()]).GetAnalyzerDiagnosticsAsync();
        var diagnostic = Assert.Single(diagnostics.Where(item => item.Id == diagnosticId));
        var actions = new List<Microsoft.CodeAnalysis.CodeActions.CodeAction>();
        var provider = new FitzCodeFixProvider();
        await provider.RegisterCodeFixesAsync(new CodeFixContext(
            document,
            diagnostic,
            (action, _) => actions.Add(action),
            CancellationToken.None));
        var action = Assert.Single(actions);
        var operation = Assert.Single(await action.GetOperationsAsync(CancellationToken.None));
        operation.Apply(workspace, CancellationToken.None);
        var changed = workspace.CurrentSolution.GetDocument(document.Id);
        Assert.NotNull(changed);
        return (await changed.GetTextAsync()).ToString();
    }

    static AdhocWorkspace CreateWorkspace(string source, out Document document)
    {
        var workspace = new AdhocWorkspace();
        var project = workspace.AddProject("Consumer", LanguageNames.CSharp)
            .WithCompilationOptions(new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                .WithUsings("System", "System.Threading", "System.Threading.Tasks"))
            .WithParseOptions(new CSharpParseOptions(LanguageVersion.Preview))
            .AddMetadataReferences(GetReferences());
        Assert.True(workspace.TryApplyChanges(project.Solution));
        document = workspace.AddDocument(project.Id, "Consumer.cs", SourceText.From(
            "global using System;\nglobal using System.Threading;\nglobal using System.Threading.Tasks;\n" + source));
        return workspace;
    }

    static IEnumerable<MetadataReference> GetReferences()
    {
        var trustedAssemblies = (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES");
        Assert.NotNull(trustedAssemblies);
        foreach (var path in trustedAssemblies.Split(Path.PathSeparator))
        {
            yield return MetadataReference.CreateFromFile(path);
        }

        yield return MetadataReference.CreateFromFile(typeof(ILeaseClient).Assembly.Location);
        yield return MetadataReference.CreateFromFile(typeof(LeaseClient).Assembly.Location);
    }
}
