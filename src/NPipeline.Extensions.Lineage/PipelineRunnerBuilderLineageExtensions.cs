using NPipeline.Execution;

namespace NPipeline.Lineage;

/// <summary>
///     Provides extension methods for enabling lineage on <see cref="PipelineRunnerBuilder" />.
/// </summary>
public static class PipelineRunnerBuilderLineageExtensions
{
    /// <summary>
    ///     Makes the runner track lineage with a <see cref="LineageService" />, for running pipelines without dependency
    ///     injection.
    /// </summary>
    /// <param name="builder">The runner builder.</param>
    /// <returns>The current PipelineRunnerBuilder instance for method chaining.</returns>
    /// <remarks>
    ///     A runner from <see cref="PipelineRunner.Create" /> or a bare <see cref="PipelineRunnerBuilder" /> uses
    ///     <see cref="NullLineage" />, which records nothing: item-level lineage fails to build, and pipeline lineage sinks
    ///     never receive a report. Under dependency injection, <c>services.AddNPipelineLineage()</c> does this instead.
    /// </remarks>
    /// <example>
    ///     <code>
    ///     var runner = new PipelineRunnerBuilder().UseLineage().Build();
    ///     </code>
    /// </example>
    public static PipelineRunnerBuilder UseLineage(this PipelineRunnerBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.WithLineage(new LineageService());
    }
}
