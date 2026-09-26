using NPipeline.Lineage;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Orchestration;

internal sealed class PipelineLineageRecordingStage(ILineage lineage)
{
    public async Task RecordAsync(
        Type definitionType,
        PipelineExecutionSetupResult setup,
        PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(definitionType);
        ArgumentNullException.ThrowIfNull(context);

        // NullLineage drops the report silently; a configured sink means the caller expected one.
        if (lineage is NullLineage && setup.PipelineLineageSink is { } sink)
        {
            var logger = context.Observability.LoggerFactory.CreateLogger(nameof(PipelineLineageRecordingStage));
            PipelineLineageRecordingLogMessages.PipelineLineageSinkIgnored(logger, sink.GetType().Name);
            return;
        }

        await lineage.RecordPipelineAsync(definitionType, setup.Graph, context, setup.PipelineLineageSink).ConfigureAwait(false);
    }
}
