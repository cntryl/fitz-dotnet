using System.Collections.Immutable;
using System.Globalization;
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
    [InlineData("await client.AcquireAsync(\"queue://realm/area/name\", TimeSpan.FromSeconds(30));", FitzDiagnostics.InvalidRouteId)]
    [InlineData("await client.ListAsync(\"lease://realm/bad*value/name\");", FitzDiagnostics.InvalidPatternId)]
    [InlineData("await client.AcquireAsync(\"lease://realm/area/name\", TimeSpan.FromSeconds(0));", FitzDiagnostics.InvalidArgumentId)]
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
            "await client.AcquireAsync(\"lease://realm/area/name\", TimeSpan.FromSeconds(30));"));

        Assert.Contains(diagnostics, diagnostic => diagnostic.Id == FitzDiagnostics.DiscardedHandleId);
    }

    [Fact]
    public async Task ShouldAcceptRetainedHandleAndNonconstantRouteGivenAnalyzerInputWhenAnalyzing()
    {
        // Arrange
        // Act
        // Assert
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz;
            class Consumer
            {
                static async Task Run(ILeaseClient client, string route)
                {
                    await using var lease = await client.AcquireAsync(route, TimeSpan.FromSeconds(30));
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
                    await using var lease = await client.AcquireAsync("queue://realm/area/name", TimeSpan.FromSeconds(0));
                }
            }
            """, FirstPartyAssembly);

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
            using Cntryl.Fitz;
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
            using Cntryl.Fitz;
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
            using Cntryl.Fitz;
            class Consumer
            {
                static async Task Run(IQueueClient client)
                {
                    _ = await client.EnqueueAsync(
                        "queue://realm/area/name",
                        ReadOnlyMemory<byte>.Empty,
                        delay: TimeSpan.FromMilliseconds(1));
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
            LeaseSource("await using var lease = await client.AcquireAsync(\"queue://realm/area/name\", TimeSpan.FromSeconds(30));"),
            FitzDiagnostics.InvalidRouteId);

        Assert.Contains("\"lease://realm/area/name\"", fixedSource, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShouldReportErrorGivenInvalidRouteWhenAnalyzing()
    {
        // Arrange
        // Act
        // Assert
        var diagnostics = await GetDiagnosticsAsync(LeaseSource(
            "await using var lease = await client.AcquireAsync(\"lease://realm/bad*value/name\", TimeSpan.FromSeconds(30));"));

        var diagnostic = Assert.Single(diagnostics.Where(item => item.Id == FitzDiagnostics.InvalidRouteId));
        Assert.Equal(DiagnosticSeverity.Error, diagnostic.Severity);
    }

    [Fact]
    public async Task ShouldNotOfferFixGivenInvalidSchemeReplacementWhenRegisteringRouteFix()
    {
        // Arrange
        using var workspace = CreateWorkspace(
            LeaseSource("await using var lease = await client.AcquireAsync(\"queue://realm\", TimeSpan.FromSeconds(30));"),
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
            LeaseSource("await client.AcquireAsync(\"lease://realm/area/name\", TimeSpan.FromSeconds(30));"),
            FitzDiagnostics.DiscardedHandleId);

        Assert.Contains("await using var lease = await client.AcquireAsync", fixedSource, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(FitzDiagnostics.InvalidRouteId, "fitz001")]
    [InlineData(FitzDiagnostics.InvalidPatternId, "fitz002")]
    [InlineData(FitzDiagnostics.DiscardedHandleId, "fitz003")]
    [InlineData(FitzDiagnostics.InvalidArgumentId, "fitz004")]
    public void ShouldBeErrorAndLinkToDocsGivenEveryDescriptorWhenInspectingMetadata(string id, string anchor)
    {
        // Arrange
        var descriptor = new FitzUsageAnalyzer().SupportedDiagnostics.Single(item => item.Id == id);

        // Act
        var link = descriptor.HelpLinkUri;

        // Assert
        Assert.Equal(DiagnosticSeverity.Error, descriptor.DefaultSeverity);
        Assert.Equal("https://github.com/cntryl/fitz-dotnet/blob/main/docs/analyzers.md#" + anchor, link);
        Assert.False(string.IsNullOrWhiteSpace(descriptor.Description.ToString(CultureInfo.InvariantCulture)));
    }

    [Fact]
    public async Task ShouldReportInvalidRouteGivenInMemoryClientInvocationWhenAnalyzing()
    {
        // Arrange
        // Act
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz;
            using Cntryl.Fitz.Testing;
            class Consumer
            {
                static async Task Run(InMemoryKvClient client)
                {
                    await using var transaction = await client.BeginAsync("queue://realm/area/name");
                }
            }
            """);

        // Assert
        Assert.Contains(diagnostics, diagnostic => diagnostic.Id == FitzDiagnostics.InvalidRouteId);
    }

    [Fact]
    public async Task ShouldReportInvalidRouteGivenUserImplementationInvocationWhenAnalyzing()
    {
        // Arrange
        // Act
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz;
            class MyKv : IKvClient
            {
                public Task<IKvTransaction> BeginAsync(string route, KvMode mode = KvMode.ReadWrite, CancellationToken ct = default) =>
                    throw new NotSupportedException();

                public Task<KvSubscription> SubscribeAsync(string pattern, CancellationToken ct = default) =>
                    throw new NotSupportedException();
            }
            class Consumer
            {
                static async Task Run(MyKv client)
                {
                    await using var transaction = await client.BeginAsync("queue://realm/area/name");
                    await using var subscription = await client.SubscribeAsync("kv://realm/bad*/name");
                }
            }
            """);

        // Assert
        Assert.Contains(diagnostics, diagnostic => diagnostic.Id == FitzDiagnostics.InvalidRouteId);
        Assert.Contains(diagnostics, diagnostic => diagnostic.Id == FitzDiagnostics.InvalidPatternId);
    }

    [Fact]
    public async Task ShouldReportInvalidRouteGivenOverrideOfInterfaceImplementationWhenAnalyzing()
    {
        // Arrange
        // Act
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz;
            class BaseKv
            {
                public virtual Task<IKvTransaction> BeginAsync(string route, KvMode mode = KvMode.ReadWrite, CancellationToken ct = default) =>
                    throw new NotSupportedException();

                public Task<KvSubscription> SubscribeAsync(string pattern, CancellationToken ct = default) =>
                    throw new NotSupportedException();
            }
            class DerivedKv : BaseKv, IKvClient
            {
                public override Task<IKvTransaction> BeginAsync(string route, KvMode mode = KvMode.ReadWrite, CancellationToken ct = default) =>
                    throw new NotSupportedException();
            }
            class Consumer
            {
                static async Task Run(DerivedKv derived)
                {
                    await using var one = await derived.BeginAsync("queue://realm/area/name");
                    await using var two = await derived.SubscribeAsync("queue://realm/area/name");
                }
            }
            """);

        // Assert
        Assert.Contains(diagnostics, diagnostic => diagnostic.Id == FitzDiagnostics.InvalidRouteId);
        Assert.Contains(diagnostics, diagnostic => diagnostic.Id == FitzDiagnostics.InvalidPatternId);
    }

    [Fact]
    public async Task ShouldReportInvalidRouteGivenClientServiceAccessorInvocationWhenAnalyzing()
    {
        // Arrange
        // Act
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz;
            class Consumer
            {
                static async Task Run(FitzClient client)
                {
                    await using var lease = await client.Lease.AcquireAsync("queue://realm/area/name", TimeSpan.FromSeconds(30));
                }
            }
            """);

        // Assert
        Assert.Contains(diagnostics, diagnostic => diagnostic.Id == FitzDiagnostics.InvalidRouteId);
    }

    [Fact]
    public async Task ShouldIgnoreUnrelatedClassGivenSameMethodNamesWhenAnalyzing()
    {
        // Arrange
        // Act
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz;
            class NotKv
            {
                public Task<IKvTransaction> BeginAsync(string route) => throw new NotSupportedException();
            }
            class Consumer
            {
                static async Task Run(NotKv client)
                {
                    await using var transaction = await client.BeginAsync("queue://realm/area/name");
                }
            }
            """);

        // Assert
        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task ShouldReportDiscardedHandleGivenDiscardAssignmentWhenAnalyzing()
    {
        // Arrange
        // Act
        var diagnostics = await GetDiagnosticsAsync(LeaseSource(
            "_ = await client.AcquireAsync(\"lease://realm/area/name\", TimeSpan.FromSeconds(30));"));

        // Assert
        var diagnostic = Assert.Single(diagnostics, item => item.Id == FitzDiagnostics.DiscardedHandleId);
        Assert.Equal("lease", diagnostic.Properties["HandleName"]);
    }

    [Fact]
    public async Task ShouldReportDiscardedHandleGivenValueTaskResultWhenAnalyzing()
    {
        // Arrange
        // Act
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz;
            class Consumer
            {
                static ValueTask<ILease> Acquire() => throw new NotSupportedException();

                static async Task Run()
                {
                    await Acquire();
                    _ = await Acquire();
                }
            }
            """);

        // Assert
        Assert.Equal(2, diagnostics.Count(item => item.Id == FitzDiagnostics.DiscardedHandleId));
    }

    [Fact]
    public async Task ShouldNotReportDiscardedHandleGivenLookalikeTaskTypeWhenAnalyzing()
    {
        // Arrange
        // Act
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz;
            namespace Lookalike
            {
                class Task<T>
                {
                    public System.Runtime.CompilerServices.TaskAwaiter<T> GetAwaiter() =>
                        System.Threading.Tasks.Task.FromResult(default(T)!).GetAwaiter();
                }
            }
            class Consumer
            {
                static Lookalike.Task<ILease> Acquire() => throw new NotSupportedException();

                static async System.Threading.Tasks.Task Run()
                {
                    await Acquire();
                }
            }
            """);

        // Assert
        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task ShouldNotReportDiscardedHandleGivenDiscardedNonHandleResultWhenAnalyzing()
    {
        // Arrange
        // Act
        var diagnostics = await GetDiagnosticsAsync("""
            using Cntryl.Fitz;
            class Consumer
            {
                static async Task Run(ILeaseClient client)
                {
                    _ = await client.ListAsync("lease://realm/area/*");
                }
            }
            """);

        // Assert
        Assert.DoesNotContain(diagnostics, diagnostic => diagnostic.Id == FitzDiagnostics.DiscardedHandleId);
    }

    [Fact]
    public async Task ShouldGiveEachHandleAUniqueNameGivenThreeDiscardedHandlesWhenApplyingFixAll()
    {
        // Arrange
        // Act
        var fixedSource = await ApplyFixAllAsync(
            LeaseSource("""
                await client.AcquireAsync("lease://realm/area/one", TimeSpan.FromSeconds(30));
                await client.AcquireAsync("lease://realm/area/two", TimeSpan.FromSeconds(30));
                await client.AcquireAsync("lease://realm/area/three", TimeSpan.FromSeconds(30));
                """),
            FitzDiagnostics.DiscardedHandleId);

        // Assert
        Assert.Contains("await using var lease = await client.AcquireAsync(\"lease://realm/area/one\"", fixedSource, StringComparison.Ordinal);
        Assert.Contains("await using var lease2 = await client.AcquireAsync(\"lease://realm/area/two\"", fixedSource, StringComparison.Ordinal);
        Assert.Contains("await using var lease3 = await client.AcquireAsync(\"lease://realm/area/three\"", fixedSource, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShouldRepairRouteAndHandleTogetherGivenOneStatementWhenApplyingFixAll()
    {
        // Arrange
        // Act
        var fixedSource = await ApplyFixAllAsync(
            LeaseSource("await client.AcquireAsync(\"queue://realm/area/one\", TimeSpan.FromSeconds(30));"),
            FitzDiagnostics.DiscardedHandleId);

        // Assert
        Assert.Contains("await using var lease = await client.AcquireAsync(\"queue://realm/area/one\"", fixedSource, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShouldWrapEmbeddedStatementInBlockGivenIfWithoutBracesWhenApplyingFix()
    {
        // Arrange
        // Act
        var fixedSource = await ApplyFirstFixAsync(
            """
            using Cntryl.Fitz;
            class Consumer
            {
                static async Task Run(ILeaseClient client, bool enabled)
                {
                    if (enabled)
                        await client.AcquireAsync("lease://realm/area/name", TimeSpan.FromSeconds(30));
                }
            }
            """,
            FitzDiagnostics.DiscardedHandleId);

        // Assert
        Assert.Contains("{", fixedSource, StringComparison.Ordinal);
        Assert.Contains("await using var lease = await client.AcquireAsync", fixedSource, StringComparison.Ordinal);
        await AssertCompilesAsync(fixedSource);
    }

    [Fact]
    public async Task ShouldKeepCommentsGivenTriviaAroundDiscardedHandleWhenApplyingFix()
    {
        // Arrange
        // Act
        var fixedSource = await ApplyFirstFixAsync(
            LeaseSource("""
                // keep the lease for the whole operation
                await client.AcquireAsync("lease://realm/area/name", TimeSpan.FromSeconds(30)); // trailing note
                """),
            FitzDiagnostics.DiscardedHandleId);

        // Assert
        Assert.Contains("// keep the lease for the whole operation", fixedSource, StringComparison.Ordinal);
        Assert.Contains("// trailing note", fixedSource, StringComparison.Ordinal);
        await AssertCompilesAsync(fixedSource);
    }

    [Fact]
    public async Task ShouldAvoidLocalNameInScopeGivenExistingLeaseVariableWhenApplyingFix()
    {
        // Arrange
        // Act
        var fixedSource = await ApplyFirstFixAsync(
            LeaseSource("""
                var lease = 1;
                await client.AcquireAsync("lease://realm/area/name", TimeSpan.FromSeconds(30));
                """),
            FitzDiagnostics.DiscardedHandleId);

        // Assert
        Assert.Contains("await using var lease2 = await client.AcquireAsync", fixedSource, StringComparison.Ordinal);
        await AssertCompilesAsync(fixedSource);
    }

    static string LeaseSource(string operation) => $$"""
        using Cntryl.Fitz;
        class Consumer
        {
            static async Task Run(ILeaseClient client)
            {
                {{operation}}
            }
        }
        """;

    static async Task<ImmutableArray<Diagnostic>> GetDiagnosticsAsync(string source, string assemblyName = ConsumerAssembly)
    {
        using var workspace = CreateWorkspace(source, out var document, assemblyName);
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

    static async Task AssertCompilesAsync(string source)
    {
        using var workspace = CreateWorkspace(source, out var document);
        var compilation = await document.Project.GetCompilationAsync();
        Assert.NotNull(compilation);
        Assert.DoesNotContain(compilation.GetDiagnostics(), diagnostic => diagnostic.Severity == DiagnosticSeverity.Error);
    }

    static async Task<string> ApplyFixAllAsync(string source, string diagnosticId)
    {
        using var workspace = CreateWorkspace(source, out var document);
        var provider = new FitzCodeFixProvider();
        var context = new FixAllContext(
            document,
            provider,
            FixAllScope.Document,
            "FitzRetainHandle",
            [diagnosticId],
            new AnalyzerDiagnosticProvider(),
            CancellationToken.None);
        var action = await provider.GetFixAllProvider().GetFixAsync(context);
        Assert.NotNull(action);
        foreach (var operation in await action.GetOperationsAsync(CancellationToken.None))
        {
            operation.Apply(workspace, CancellationToken.None);
        }

        var changed = workspace.CurrentSolution.GetDocument(document.Id);
        Assert.NotNull(changed);
        var text = (await changed.GetTextAsync()).ToString();
        await AssertCompilesAsync(text);
        return text;
    }

    sealed class AnalyzerDiagnosticProvider : FixAllContext.DiagnosticProvider
    {
        public override Task<IEnumerable<Diagnostic>> GetAllDiagnosticsAsync(Project project, CancellationToken ct) =>
            GetProjectDiagnosticsAsync(project, ct);

        public override async Task<IEnumerable<Diagnostic>> GetProjectDiagnosticsAsync(Project project, CancellationToken ct)
        {
            var compilation = await project.GetCompilationAsync(ct);
            Assert.NotNull(compilation);
            return await compilation.WithAnalyzers([new FitzUsageAnalyzer()]).GetAnalyzerDiagnosticsAsync(ct);
        }

        public override async Task<IEnumerable<Diagnostic>> GetDocumentDiagnosticsAsync(Document document, CancellationToken ct)
        {
            var tree = await document.GetSyntaxTreeAsync(ct);
            var all = await GetProjectDiagnosticsAsync(document.Project, ct);
            return all.Where(diagnostic => diagnostic.Location.SourceTree == tree);
        }
    }

    // Most tests compile as an ordinary consumer that sees only the public surface. The
    // concrete-client test compiles under the InternalsVisibleTo name instead, because the
    // concrete domain clients are internal and only first-party code can invoke them.
    const string ConsumerAssembly = "Consumer";
    const string FirstPartyAssembly = "Cntryl.Fitz.Core.Tests";

    static AdhocWorkspace CreateWorkspace(string source, out Document document, string assemblyName = ConsumerAssembly)
    {
        var workspace = new AdhocWorkspace();
        var project = workspace.AddProject(assemblyName, LanguageNames.CSharp)
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
        yield return MetadataReference.CreateFromFile(typeof(FitzClient).Assembly.Location);
    }
}
