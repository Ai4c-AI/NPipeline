using System.Text.Json;
using Microsoft.Extensions.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Lineage;

/// <summary>
///     Provides extension methods for configuring lineage on <see cref="PipelineBuilder" />.
/// </summary>
public static class PipelineBuilderLineageExtensions
{
    /// <summary>
    ///     Configures the pipeline to use a <see cref="LoggingPipelineLineageSink" /> for pipeline-level lineage reporting.
    /// </summary>
    /// <param name="builder">The pipeline builder.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    /// <remarks>
    ///     The sink is registered by type, so the run's lineage factory constructs it with the run's logging: the container's
    ///     <see cref="ILogger{TCategoryName}" /> when the pipeline runs through dependency injection, and the context's
    ///     <see cref="ILoggerFactory" /> otherwise. To log through a specific logger factory or customize the JSON output, use
    ///     <see cref="UseLoggingPipelineLineageSink(PipelineBuilder, ILoggerFactory, JsonSerializerOptions?)" />.
    /// </remarks>
    public static PipelineBuilder UseLoggingPipelineLineageSink(this PipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.AddPipelineLineageSink<LoggingPipelineLineageSink>();
    }

    /// <summary>
    ///     Configures the pipeline to use a <see cref="LoggingPipelineLineageSink" /> that logs pipeline-level lineage reports
    ///     through the specified logger factory.
    /// </summary>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="loggerFactory">The logger factory to create the sink's logger from.</param>
    /// <param name="jsonOptions">Optional JSON serialization options for the sink.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public static PipelineBuilder UseLoggingPipelineLineageSink(
        this PipelineBuilder builder,
        ILoggerFactory loggerFactory,
        JsonSerializerOptions? jsonOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(loggerFactory);

        var sink = new LoggingPipelineLineageSink(loggerFactory.CreateLogger<LoggingPipelineLineageSink>(), jsonOptions);
        return builder.AddPipelineLineageSink(sink);
    }
}
