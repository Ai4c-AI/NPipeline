using System.Collections.Immutable;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Connectors.Postgres.Analyzers;

/// <summary>
///     Warns when a PostgreSQL source checkpoints (<c>CheckpointStrategy.Offset</c> or <c>InMemory</c>) with a query that has
///     no <c>ORDER BY</c>. Both strategies resume by skipping the number of rows already read, which only means the same
///     rows when the query returns them in a stable order.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class PostgresCheckpointOrderingAnalyzer : DiagnosticAnalyzer
{
    /// <summary>The diagnostic's id.</summary>
    public const string PostgresCheckpointOrderingId = "NP9501";

    private static readonly DiagnosticDescriptor Rule = new(
        PostgresCheckpointOrderingId,
        "PostgreSQL source with checkpointing requires ORDER BY clause",
        "Checkpointing resumes by skipping the rows already read, so the query needs an ORDER BY on a unique, stable key; without one, a restart can skip or repeat rows",
        "Reliability",
        DiagnosticSeverity.Warning,
        true,
        "CheckpointStrategy.Offset and InMemory record how many rows were read and skip that many when the read restarts. That only lands on the same rows when the query orders them by a unique, stable key (e.g. id).");

    private static readonly Regex OrderBy = new(@"\bORDER\s+BY\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(Rule);

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
        context.RegisterSyntaxNodeAction(AnalyzeOptions, SyntaxKind.ObjectCreationExpression);
    }

    /// <summary><c>PostgresConnector.Source(connection, query, configure)</c> and the DI factory's <c>CreateSourceNode(query, configure)</c>.</summary>
    private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
    {
        var invocation = (InvocationExpressionSyntax)context.Node;

        if (context.SemanticModel.GetSymbolInfo(invocation, context.CancellationToken).Symbol is not IMethodSymbol method)
            return;

        var queryIndex = method switch
        {
            { Name: "Source", ContainingType.Name: "PostgresConnector" } => 1,
            { Name: "CreateSourceNode", ContainingType.Name: "PostgresSourceNodeFactory" or "IPostgresSourceNodeFactory" } => 0,
            _ => -1,
        };

        if (queryIndex < 0 || !InConnectorNamespace(method.ContainingType))
            return;

        var arguments = invocation.ArgumentList.Arguments;

        if (arguments.Count <= queryIndex || Constant(arguments[queryIndex].Expression, context.SemanticModel) is not { } query)
            return;

        // The strategy is set in the configure lambda: o => o with { CheckpointStrategy = … }.
        var strategy = arguments.Skip(queryIndex + 1).Select(a => Strategy(a.Expression, context.SemanticModel)).FirstOrDefault(s => s is not null);
        Report(context, invocation.GetLocation(), query, strategy);
    }

    /// <summary><c>new PostgresReadOptions { Query = …, CheckpointStrategy = … }</c>.</summary>
    private static void AnalyzeOptions(SyntaxNodeAnalysisContext context)
    {
        var creation = (ObjectCreationExpressionSyntax)context.Node;

        if (context.SemanticModel.GetTypeInfo(creation, context.CancellationToken).Type is not { Name: "PostgresReadOptions" } type || !InConnectorNamespace(type))
            return;

        var query = creation.Initializer?.Expressions.OfType<AssignmentExpressionSyntax>()
            .Where(a => a.Left is IdentifierNameSyntax { Identifier.Text: "Query" })
            .Select(a => Constant(a.Right, context.SemanticModel))
            .FirstOrDefault();

        if (query is not null)
            Report(context, creation.GetLocation(), query, Strategy(creation, context.SemanticModel));
    }

    private static void Report(SyntaxNodeAnalysisContext context, Location location, string query, string? strategy)
    {
        if (strategy is "Offset" or "InMemory" && !OrderBy.IsMatch(query))
            context.ReportDiagnostic(Diagnostic.Create(Rule, location));
    }

    /// <summary>The strategy assigned to <c>CheckpointStrategy</c> anywhere inside <paramref name="expression" />.</summary>
    private static string? Strategy(SyntaxNode expression, SemanticModel model)
    {
        foreach (var assignment in expression.DescendantNodesAndSelf().OfType<AssignmentExpressionSyntax>())
        {
            if (assignment.Left is not IdentifierNameSyntax { Identifier.Text: "CheckpointStrategy" })
                continue;

            if (assignment.Right is MemberAccessExpressionSyntax member)
                return member.Name.Identifier.Text;

            if (model.GetSymbolInfo(assignment.Right).Symbol is IFieldSymbol { ContainingType.TypeKind: TypeKind.Enum } field)
                return field.Name;
        }

        return null;
    }

    private static string? Constant(ExpressionSyntax expression, SemanticModel model) =>
        expression is InterpolatedStringExpressionSyntax ? null : model.GetConstantValue(expression).Value as string;

    private static bool InConnectorNamespace(ITypeSymbol type) =>
        type.ContainingNamespace?.ToDisplayString().StartsWith("NPipeline.Connectors.Postgres", StringComparison.Ordinal) == true;
}
