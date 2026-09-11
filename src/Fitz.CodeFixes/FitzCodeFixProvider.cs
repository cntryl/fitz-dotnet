using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Cntryl.Fitz.CodeFixes;

[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(FitzCodeFixProvider)), Shared]
public sealed class FitzCodeFixProvider : CodeFixProvider
{
    const string InvalidRouteId = "FITZ001";
    const string InvalidPatternId = "FITZ002";
    const string DiscardedHandleId = "FITZ003";

    public override ImmutableArray<string> FixableDiagnosticIds =>
        [InvalidRouteId, InvalidPatternId, DiscardedHandleId];

    public override FixAllProvider GetFixAllProvider() => WellKnownFixAllProviders.BatchFixer;

    public override async Task RegisterCodeFixesAsync(CodeFixContext context)
    {
        var root = await context.Document.GetSyntaxRootAsync(context.CancellationToken).ConfigureAwait(false);
        if (root is null)
        {
            return;
        }

        foreach (var diagnostic in context.Diagnostics)
        {
            if (diagnostic.Id is InvalidRouteId or InvalidPatternId)
            {
                RegisterAddressFix(context, root, diagnostic);
            }
            else if (diagnostic.Id == DiscardedHandleId)
            {
                RegisterHandleFix(context, root, diagnostic);
            }
        }
    }

    static void RegisterAddressFix(CodeFixContext context, SyntaxNode root, Diagnostic diagnostic)
    {
        var literal = root.FindNode(diagnostic.Location.SourceSpan).DescendantNodesAndSelf()
            .OfType<LiteralExpressionSyntax>().FirstOrDefault();
        if (literal is null ||
            !diagnostic.Properties.TryGetValue("ExpectedScheme", out var scheme) || scheme is null)
        {
            return;
        }

        if (!diagnostic.Properties.TryGetValue("SuggestedAddress", out var corrected) || corrected is null)
        {
            return;
        }

        context.RegisterCodeFix(
            CodeAction.Create(
                $"Use '{scheme}://' scheme",
                ct => ReplaceLiteralAsync(context.Document, root, literal, corrected, ct),
                $"FitzAddressScheme:{scheme}"),
            diagnostic);
    }

    static void RegisterHandleFix(CodeFixContext context, SyntaxNode root, Diagnostic diagnostic)
    {
        var statement = root.FindNode(diagnostic.Location.SourceSpan).FirstAncestorOrSelf<ExpressionStatementSyntax>();
        if (statement?.Expression is not AwaitExpressionSyntax awaitExpression)
        {
            return;
        }

        var preferredName = diagnostic.Properties.TryGetValue("HandleName", out var name) ? name : null;
        preferredName ??= "handle";
        context.RegisterCodeFix(
            CodeAction.Create(
                $"Retain and dispose the {preferredName}",
                ct => ReplaceStatementAsync(context.Document, root, statement, awaitExpression, preferredName, ct),
                "FitzRetainHandle"),
            diagnostic);
    }

    static Task<Document> ReplaceLiteralAsync(
        Document document,
        SyntaxNode root,
        LiteralExpressionSyntax literal,
        string corrected,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var replacement = SyntaxFactory.LiteralExpression(
            SyntaxKind.StringLiteralExpression,
            SyntaxFactory.Literal(corrected)).WithTriviaFrom(literal);
        return Task.FromResult(document.WithSyntaxRoot(root.ReplaceNode(literal, replacement)));
    }

    static async Task<Document> ReplaceStatementAsync(
        Document document,
        SyntaxNode root,
        ExpressionStatementSyntax statement,
        AwaitExpressionSyntax awaitExpression,
        string preferredName,
        CancellationToken ct)
    {
        var semanticModel = await document.GetSemanticModelAsync(ct).ConfigureAwait(false);
        var name = preferredName;
        var suffix = 2;
        while (semanticModel?.LookupSymbols(statement.SpanStart, name: name).Length > 0)
        {
            name = preferredName + suffix++;
        }

        var replacement = SyntaxFactory.ParseStatement($"await using var {name} = {awaitExpression};")
            .WithTriviaFrom(statement);
        return document.WithSyntaxRoot(root.ReplaceNode(statement, replacement));
    }
}
