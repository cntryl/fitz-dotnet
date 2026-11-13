using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Formatting;

namespace Cntryl.Fitz.CodeFixes;

[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(FitzCodeFixProvider)), Shared]
public sealed class FitzCodeFixProvider : CodeFixProvider
{
    const string InvalidRouteId = "FITZ001";
    const string InvalidPatternId = "FITZ002";
    const string DiscardedHandleId = "FITZ003";

    const string RetainHandleKey = "FitzRetainHandle";
    const string AddressSchemeKeyPrefix = "FitzAddressScheme:";

    public override ImmutableArray<string> FixableDiagnosticIds =>
        [InvalidRouteId, InvalidPatternId, DiscardedHandleId];

    // Every fix is computed against the original document, so the stock batch fixer would give
    // two discarded handles in one method the same local name. The Fitz provider fixes all the
    // occurrences in a document together instead, so it can hand each one a unique name.
    public override FixAllProvider GetFixAllProvider() => FitzFixAllProvider.Instance;

    public override async Task RegisterCodeFixesAsync(CodeFixContext context)
    {
        var root = await context.Document.GetSyntaxRootAsync(context.CancellationToken).ConfigureAwait(false);
        if (root is null)
        {
            return;
        }

        foreach (var diagnostic in context.Diagnostics)
        {
            var key = GetEquivalenceKey(root, diagnostic);
            if (key is null)
            {
                continue;
            }

            var title = key == RetainHandleKey
                ? $"Retain and dispose the {GetHandleName(diagnostic)}"
                : $"Use '{key.Substring(AddressSchemeKeyPrefix.Length)}://' scheme";
            context.RegisterCodeFix(
                CodeAction.Create(
                    title,
                    ct => FixDocumentAsync(context.Document, [diagnostic], ct),
                    key),
                diagnostic);
        }
    }

    /// <summary>
    /// The equivalence key of the fix a diagnostic gets, or null when no fix applies. Fix All
    /// applies only the diagnostics whose key matches the fix the user chose.
    /// </summary>
    static string? GetEquivalenceKey(SyntaxNode root, Diagnostic diagnostic)
    {
        if (diagnostic.Id == DiscardedHandleId)
        {
            return FindDiscardedStatement(root, diagnostic) is null ? null : RetainHandleKey;
        }

        if (diagnostic.Id is InvalidRouteId or InvalidPatternId &&
            FindLiteral(root, diagnostic) is not null &&
            diagnostic.Properties.TryGetValue("ExpectedScheme", out var scheme) && scheme is not null &&
            diagnostic.Properties.TryGetValue("SuggestedAddress", out var corrected) && corrected is not null)
        {
            return AddressSchemeKeyPrefix + scheme;
        }

        return null;
    }

    static string GetHandleName(Diagnostic diagnostic) =>
        diagnostic.Properties.TryGetValue("HandleName", out var name) && name is not null ? name : "handle";

    static ExpressionStatementSyntax? FindDiscardedStatement(SyntaxNode root, Diagnostic diagnostic)
    {
        var statement = root.FindNode(diagnostic.Location.SourceSpan).FirstAncestorOrSelf<ExpressionStatementSyntax>();
        return statement?.Expression is AwaitExpressionSyntax ? statement : null;
    }

    static LiteralExpressionSyntax? FindLiteral(SyntaxNode root, Diagnostic diagnostic)
    {
        var node = root.FindNode(diagnostic.Location.SourceSpan);
        return node switch
        {
            LiteralExpressionSyntax literal => literal,
            ArgumentSyntax { Expression: LiteralExpressionSyntax literal } => literal,
            _ => null,
        };
    }

    /// <summary>
    /// Applies the fixes for <paramref name="diagnostics"/> to one document in a single pass, so
    /// the names chosen for retained handles are unique across all of them.
    /// </summary>
    static async Task<Document> FixDocumentAsync(
        Document document,
        IReadOnlyCollection<Diagnostic> diagnostics,
        CancellationToken ct)
    {
        var root = await document.GetSyntaxRootAsync(ct).ConfigureAwait(false);
        var semanticModel = await document.GetSemanticModelAsync(ct).ConfigureAwait(false);
        if (root is null)
        {
            return document;
        }

        var literals = new Dictionary<LiteralExpressionSyntax, string>();
        var statements = new Dictionary<ExpressionStatementSyntax, string>();
        foreach (var diagnostic in diagnostics.OrderBy(item => item.Location.SourceSpan.Start))
        {
            if (diagnostic.Id == DiscardedHandleId)
            {
                if (FindDiscardedStatement(root, diagnostic) is { } statement)
                {
                    statements[statement] = GetHandleName(diagnostic);
                }
            }
            else if (FindLiteral(root, diagnostic) is { } literal &&
                diagnostic.Properties.TryGetValue("SuggestedAddress", out var corrected) && corrected is not null)
            {
                literals[literal] = corrected;
            }
        }

        var names = ChooseNames(statements, semanticModel);
        var changed = root.ReplaceNodes(
            literals.Keys.Cast<SyntaxNode>().Concat(statements.Keys),
            (original, rewritten) => original switch
            {
                LiteralExpressionSyntax literal => SyntaxFactory.LiteralExpression(
                    SyntaxKind.StringLiteralExpression,
                    SyntaxFactory.Literal(literals[literal])).WithTriviaFrom(literal),
                ExpressionStatementSyntax statement => RetainHandle((ExpressionStatementSyntax)rewritten, names[statement]),
                _ => rewritten,
            });

        var updated = document.WithSyntaxRoot(changed);
        return await Formatter.FormatAsync(updated, Formatter.Annotation, cancellationToken: ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Picks a local name for each retained handle that collides with nothing already in scope
    /// and with no name chosen for another handle in the same member.
    /// </summary>
    static Dictionary<ExpressionStatementSyntax, string> ChooseNames(
        Dictionary<ExpressionStatementSyntax, string> statements,
        SemanticModel? semanticModel)
    {
        var chosen = new Dictionary<ExpressionStatementSyntax, string>();
        var takenByMember = new Dictionary<SyntaxNode, HashSet<string>>();
        foreach (var pair in statements.OrderBy(item => item.Key.SpanStart))
        {
            var statement = pair.Key;
            var member = statement.Ancestors().FirstOrDefault(static ancestor => ancestor is MemberDeclarationSyntax or CompilationUnitSyntax)
                ?? statement;
            if (!takenByMember.TryGetValue(member, out var taken))
            {
                taken = [with(StringComparer.Ordinal)];
                takenByMember.Add(member, taken);
            }

            var name = pair.Value;
            var suffix = 2;
            while (taken.Contains(name) || semanticModel?.LookupSymbols(statement.SpanStart, name: name).Length > 0)
            {
                name = pair.Value + suffix++;
            }

            taken.Add(name);
            chosen.Add(statement, name);
        }

        return chosen;
    }

    /// <summary>
    /// Rewrites <c>await x;</c> as <c>await using var name = await x;</c>, keeping the original
    /// statement's trivia. An embedded statement, such as the body of an <c>if</c>, cannot hold a
    /// declaration, so it is wrapped in a block.
    /// </summary>
    static StatementSyntax RetainHandle(ExpressionStatementSyntax statement, string name)
    {
        var awaitExpression = (AwaitExpressionSyntax)statement.Expression;
        var declaration = SyntaxFactory.LocalDeclarationStatement(
            SyntaxFactory.Token(SyntaxKind.AwaitKeyword).WithTrailingTrivia(SyntaxFactory.Space),
            SyntaxFactory.Token(SyntaxKind.UsingKeyword).WithTrailingTrivia(SyntaxFactory.Space),
            default,
            SyntaxFactory.VariableDeclaration(
                SyntaxFactory.IdentifierName(SyntaxFactory.Identifier(
                    SyntaxFactory.TriviaList(),
                    "var",
                    SyntaxFactory.TriviaList(SyntaxFactory.Space))),
                SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.VariableDeclarator(
                        SyntaxFactory.Identifier(name),
                        null,
                        SyntaxFactory.EqualsValueClause(
                            SyntaxFactory.Token(
                                SyntaxFactory.TriviaList(SyntaxFactory.Space),
                                SyntaxKind.EqualsToken,
                                SyntaxFactory.TriviaList(SyntaxFactory.Space)),
                            awaitExpression.WithoutLeadingTrivia())))),
            statement.SemicolonToken.WithoutTrivia());

        if (statement.Parent is BlockSyntax or SwitchSectionSyntax or GlobalStatementSyntax)
        {
            return declaration
                .WithLeadingTrivia(statement.GetLeadingTrivia())
                .WithTrailingTrivia(statement.GetTrailingTrivia());
        }

        return SyntaxFactory.Block(declaration)
            .WithLeadingTrivia(statement.GetLeadingTrivia())
            .WithTrailingTrivia(statement.GetTrailingTrivia())
            .WithAdditionalAnnotations(Formatter.Annotation);
    }

    sealed class FitzFixAllProvider : FixAllProvider
    {
        internal static readonly FitzFixAllProvider Instance = new();

        public override IEnumerable<FixAllScope> GetSupportedFixAllScopes() =>
            [FixAllScope.Document, FixAllScope.Project, FixAllScope.Solution];

        public override Task<CodeAction?> GetFixAsync(FixAllContext fixAllContext)
        {
            if (fixAllContext is null)
            {
                throw new ArgumentNullException(nameof(fixAllContext));
            }

            var title = fixAllContext.CodeActionEquivalenceKey == RetainHandleKey
                ? "Retain and dispose every discarded handle"
                : "Fix every Fitz address";
            return Task.FromResult<CodeAction?>(CodeAction.Create(
                title,
                ct => FixAllAsync(fixAllContext, ct),
                fixAllContext.CodeActionEquivalenceKey));
        }

        static async Task<Solution> FixAllAsync(FixAllContext fixAllContext, CancellationToken ct)
        {
            var solution = fixAllContext.Solution;
            foreach (var document in GetDocuments(fixAllContext))
            {
                var root = await document.GetSyntaxRootAsync(ct).ConfigureAwait(false);
                if (root is null)
                {
                    continue;
                }

                var diagnostics = (await fixAllContext.GetDocumentDiagnosticsAsync(document).ConfigureAwait(false))
                    .Where(diagnostic => GetEquivalenceKey(root, diagnostic) == fixAllContext.CodeActionEquivalenceKey)
                    .ToList();
                if (diagnostics.Count == 0)
                {
                    continue;
                }

                var fixedDocument = await FixDocumentAsync(document, diagnostics, ct).ConfigureAwait(false);
                var fixedRoot = await fixedDocument.GetSyntaxRootAsync(ct).ConfigureAwait(false);
                if (fixedRoot is not null)
                {
                    solution = solution.WithDocumentSyntaxRoot(document.Id, fixedRoot);
                }
            }

            return solution;
        }

        static IEnumerable<Document> GetDocuments(FixAllContext fixAllContext) => fixAllContext.Scope switch
        {
            FixAllScope.Document when fixAllContext.Document is { } document => [document],
            FixAllScope.Project => fixAllContext.Project.Documents,
            FixAllScope.Solution => fixAllContext.Solution.Projects.SelectMany(static project => project.Documents),
            _ => [],
        };
    }
}
