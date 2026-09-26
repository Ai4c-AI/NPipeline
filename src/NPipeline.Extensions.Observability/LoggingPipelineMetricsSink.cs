using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Observability.Metrics;

namespace NPipeline.Observability;

/// <summary>
///     Sink that logs pipeline metrics to an ILogger.
/// </summary>
public sealed class LoggingPipelineMetricsSink : IPipelineMetricsSink
{
    // LoggerMessage delegates for high-performance logging - pipeline level
    private static readonly Action<ILogger, string, Guid, double, long?, long?, Exception?> s_logPipelineSuccess =
        LoggerMessage.Define<string, Guid, double, long?, long?>(
            LogLevel.Information,
            new EventId(1, nameof(LoggingPipelineMetricsSink)),
            "Pipeline {PipelineName} (RunId: {RunId}) completed successfully in {DurationMs:F3}ms. Items in: {ItemsIn}, items out: {ItemsOut}");

    private static readonly Action<ILogger, string, Guid, long?, long?, string, Exception?> s_logPipelineFailure =
        LoggerMessage.Define<string, Guid, long?, long?, string>(
            LogLevel.Error,
            new EventId(2, nameof(LoggingPipelineMetricsSink)),
            "Pipeline {PipelineName} (RunId: {RunId}) failed. Items in: {ItemsIn}, items out: {ItemsOut}. Exception: {ExceptionMessage}");

    private static readonly Action<ILogger, string, Guid, double, Exception?> s_logPipelineSuccessWithoutCounts =
        LoggerMessage.Define<string, Guid, double>(
            LogLevel.Information,
            new EventId(10, nameof(LoggingPipelineMetricsSink)),
            "Pipeline {PipelineName} (RunId: {RunId}) completed successfully in {DurationMs:F3}ms. Item counts were not recorded; "
            + "configure its source and sink nodes with WithObservability to record them");

    private static readonly Action<ILogger, string, Guid, string, Exception?> s_logPipelineFailureWithoutCounts =
        LoggerMessage.Define<string, Guid, string>(
            LogLevel.Error,
            new EventId(11, nameof(LoggingPipelineMetricsSink)),
            "Pipeline {PipelineName} (RunId: {RunId}) failed. Exception: {ExceptionMessage}");

    private static readonly Action<ILogger, string, long, long, double, Exception?> s_logNodeSuccess =
        LoggerMessage.Define<string, long, long, double>(
            LogLevel.Information,
            new EventId(3, nameof(LoggingPipelineMetricsSink)),
            "  Node {NodeId}: Processed {ItemsProcessed} items, emitted {ItemsEmitted} items in {DurationMs:F3}ms");

    private static readonly Action<ILogger, string, long, string, Exception?> s_logNodeFailure =
        LoggerMessage.Define<string, long, string>(
            LogLevel.Warning,
            new EventId(4, nameof(LoggingPipelineMetricsSink)),
            "  Node {NodeId}: Failed after processing {ItemsProcessed} items. Exception: {ExceptionMessage}");

    private static readonly Action<ILogger, string, int, Exception?> s_logNodeRetryCount =
        LoggerMessage.Define<string, int>(
            LogLevel.Information,
            new EventId(5, nameof(LoggingPipelineMetricsSink)),
            "    Node {NodeId} required {RetryCount} retry attempts");

    private static readonly Action<ILogger, string, long, long, long, Exception?> s_logNodeResilience =
        LoggerMessage.Define<string, long, long, long>(
            LogLevel.Information,
            new EventId(9, nameof(LoggingPipelineMetricsSink)),
            "    Node {NodeId} retried {RetryEvents} times, gave up retrying {RetriesExhausted} times, and its circuit breaker opened {CircuitBreakerTrips} times");

    private static readonly Action<ILogger, string, double, Exception?> s_logNodeThroughput =
        LoggerMessage.Define<string, double>(
            LogLevel.Debug,
            new EventId(6, nameof(LoggingPipelineMetricsSink)),
            "    Node {NodeId} throughput: {Throughput:F2} items/sec");

    private static readonly Action<ILogger, string, double, Exception?> s_logNodeAverageTime =
        LoggerMessage.Define<string, double>(
            LogLevel.Debug,
            new EventId(7, nameof(LoggingPipelineMetricsSink)),
            "    Node {NodeId} average item time: {AverageMs:F2} ms");

    private static readonly Action<ILogger, double, Exception?> s_logOverallThroughput =
        LoggerMessage.Define<double>(
            LogLevel.Information,
            new EventId(8, nameof(LoggingPipelineMetricsSink)),
            "Overall pipeline throughput: {Throughput:F2} items/sec");

    private readonly ILogger _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="LoggingPipelineMetricsSink" /> class.
    /// </summary>
    /// <param name="logger">The logger to write metrics to.</param>
    public LoggingPipelineMetricsSink(ILogger<LoggingPipelineMetricsSink>? logger = null)
    {
        _logger = logger ?? NullLogger<LoggingPipelineMetricsSink>.Instance;
    }

    /// <summary>
    ///     Asynchronously records pipeline metrics.
    /// </summary>
    /// <param name="pipelineMetrics">The pipeline metrics to record.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    public Task RecordAsync(IPipelineMetrics pipelineMetrics, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(pipelineMetrics);

        using (_logger.BeginScope(new Dictionary<string, object?>
               {
                   ["PipelineName"] = pipelineMetrics.PipelineName,
                   ["RunId"] = pipelineMetrics.RunId,
                   ["Success"] = pipelineMetrics.Success,
                   ["TotalItemsProcessed"] = pipelineMetrics.TotalItemsProcessed,
                   ["ItemsIn"] = pipelineMetrics.ItemsIn,
                   ["ItemsOut"] = pipelineMetrics.ItemsOut,
                   ["DurationMs"] = pipelineMetrics.DurationMs,
               }))
        {
            // Items in and out describe the pipeline as a whole. TotalItemsProcessed counts an item once per node it
            // passes through, so it is not reported as the pipeline's item count.
            var countsRecorded = pipelineMetrics.ItemsIn.HasValue || pipelineMetrics.ItemsOut.HasValue;
            var exceptionMessage = pipelineMetrics.Exception?.Message ?? "Unknown error";

            switch (pipelineMetrics.Success, countsRecorded)
            {
                case (true, true):
                    s_logPipelineSuccess(_logger, pipelineMetrics.PipelineName, pipelineMetrics.RunId, pipelineMetrics.DurationMs ?? 0.0,
                        pipelineMetrics.ItemsIn, pipelineMetrics.ItemsOut, null);
                    break;
                case (true, false):
                    s_logPipelineSuccessWithoutCounts(_logger, pipelineMetrics.PipelineName, pipelineMetrics.RunId, pipelineMetrics.DurationMs ?? 0.0,
                        null);
                    break;
                case (false, true):
                    s_logPipelineFailure(_logger, pipelineMetrics.PipelineName, pipelineMetrics.RunId, pipelineMetrics.ItemsIn, pipelineMetrics.ItemsOut,
                        exceptionMessage, null);
                    break;
                default:
                    s_logPipelineFailureWithoutCounts(_logger, pipelineMetrics.PipelineName, pipelineMetrics.RunId, exceptionMessage, null);
                    break;
            }

            // Log node-level metrics
            foreach (var nodeMetric in pipelineMetrics.NodeMetrics)
            {
                if (nodeMetric.Success)
                {
                    s_logNodeSuccess(
                        _logger,
                        nodeMetric.NodeId,
                        nodeMetric.ItemsProcessed,
                        nodeMetric.ItemsEmitted,
                        nodeMetric.DurationMs ?? 0.0,
                        null);
                }
                else
                {
                    s_logNodeFailure(
                        _logger,
                        nodeMetric.NodeId,
                        nodeMetric.ItemsProcessed,
                        nodeMetric.Exception?.Message ?? "Unknown error",
                        null);
                }

                if (nodeMetric.RetryCount > 0)
                    s_logNodeRetryCount(_logger, nodeMetric.NodeId, nodeMetric.RetryCount, null);

                if (nodeMetric.RetriesExhausted > 0 || nodeMetric.CircuitBreakerTrips > 0)
                    s_logNodeResilience(_logger, nodeMetric.NodeId, nodeMetric.RetryEvents, nodeMetric.RetriesExhausted, nodeMetric.CircuitBreakerTrips, null);

                if (nodeMetric.ThroughputItemsPerSec.HasValue)
                    s_logNodeThroughput(_logger, nodeMetric.NodeId, nodeMetric.ThroughputItemsPerSec.Value, null);

                if (nodeMetric.AverageItemProcessingMs.HasValue)
                    s_logNodeAverageTime(_logger, nodeMetric.NodeId, nodeMetric.AverageItemProcessingMs.Value, null);
            }

            // Overall throughput is the items leaving the pipeline per second (or entering it, when only sources were
            // observed).
            if (pipelineMetrics.DurationMs is > 0 && (pipelineMetrics.ItemsOut ?? pipelineMetrics.ItemsIn) is { } items)
            {
                var overallThroughput = items / (pipelineMetrics.DurationMs.Value / 1000.0);

                s_logOverallThroughput(_logger, overallThroughput, null);
            }
        }

        return Task.CompletedTask;
    }
}
