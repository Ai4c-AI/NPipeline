namespace NPipeline.Observability.Metrics;

/// <summary>
///     Represents performance and throughput metrics for an entire pipeline execution.
/// </summary>
/// <remarks>
///     <para>
///         Pipeline metrics provide high-level insights into overall pipeline performance:
///         - Total execution time
///         - Success/failure status
///         - Total items processed
///         - Per-node metrics for granular analysis
///     </para>
///     <para>
///         Use this interface to monitor pipeline health, identify bottlenecks, and track
///         historical performance trends.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Access metrics after pipeline execution
/// var context = new PipelineContext();
/// await runner.RunAsync&lt;MyPipeline&gt;(context);
/// 
/// if (context.Metrics is IPipelineMetrics metrics)
/// {
///     Console.WriteLine($"Pipeline: {metrics.PipelineName}");
///     Console.WriteLine($"Items in: {metrics.ItemsIn}, items out: {metrics.ItemsOut}");
///     Console.WriteLine($"Duration: {metrics.DurationMs}ms");
///     Console.WriteLine($"Success: {metrics.Success}");
/// 
///     foreach (var nodeMetric in metrics.NodeMetrics)
///     {
///         Console.WriteLine($"  {nodeMetric.NodeName}: {nodeMetric.ItemsProcessed} items");
///     }
/// }
/// </code>
/// </example>
public interface IPipelineMetrics
{
    /// <summary>
    ///     The name of the pipeline.
    /// </summary>
    string PipelineName { get; }

    /// <summary>
    ///     The unique pipeline identity for this pipeline execution context.
    /// </summary>
    Guid PipelineId { get; }

    /// <summary>
    ///     The unique identifier for this pipeline run.
    /// </summary>
    Guid RunId { get; }

    /// <summary>
    ///     The timestamp when the pipeline execution started.
    /// </summary>
    DateTimeOffset StartTime { get; }

    /// <summary>
    ///     The timestamp when the pipeline execution completed.
    /// </summary>
    DateTimeOffset? EndTime { get; }

    /// <summary>
    ///     The total duration of the pipeline execution in milliseconds.
    /// </summary>
    double? DurationMs { get; }

    /// <summary>
    ///     Whether the pipeline execution was successful.
    /// </summary>
    bool Success { get; }

    /// <summary>
    ///     The sum of <see cref="INodeMetrics.ItemsProcessed" /> across all nodes in the pipeline.
    /// </summary>
    /// <remarks>
    ///     An item that passes through several nodes is counted once per node, so this is not the number of items the
    ///     pipeline handled. Use <see cref="ItemsIn" /> and <see cref="ItemsOut" /> for that.
    /// </remarks>
    long TotalItemsProcessed { get; }

    /// <summary>
    ///     The number of items that entered the pipeline: the items its source nodes emitted.
    /// </summary>
    /// <remarks>
    ///     Null when no source node recorded item counts. Item counts are recorded for nodes configured with
    ///     <c>WithObservability</c>.
    /// </remarks>
    long? ItemsIn => null;

    /// <summary>
    ///     The number of items that left the pipeline: the items its sink nodes processed.
    /// </summary>
    /// <remarks>
    ///     Null when no sink node recorded item counts. Item counts are recorded for nodes configured with
    ///     <c>WithObservability</c>.
    /// </remarks>
    long? ItemsOut => null;

    /// <summary>
    ///     Metrics for individual nodes in the pipeline.
    /// </summary>
    /// <remarks>
    ///     Each entry in this list corresponds to a node in the pipeline definition.
    ///     Use per-node metrics to identify which nodes are bottlenecks or experiencing errors.
    /// </remarks>
    IReadOnlyList<INodeMetrics> NodeMetrics { get; }

    /// <summary>
    ///     Any exception that occurred during execution.
    /// </summary>
    /// <remarks>
    ///     If <c>Success</c> is <c>false</c>, this will contain the exception that caused the failure.
    /// </remarks>
    Exception? Exception { get; }
}
