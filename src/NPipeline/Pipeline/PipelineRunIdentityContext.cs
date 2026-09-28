namespace NPipeline.Pipeline;

/// <summary>
///     Run-level identity state for a pipeline execution.
/// </summary>
public sealed class PipelineRunIdentityContext
{
    internal PipelineRunIdentityContext(DateTime pipelineStartTimeUtc)
    {
        PipelineStartTimeUtc = pipelineStartTimeUtc;
    }

    /// <summary>
    ///     The pipeline-level UTC start timestamp.
    /// </summary>
    public DateTime PipelineStartTimeUtc { get; internal set; }

    /// <summary>
    ///     Unique pipeline identity for this execution context.
    /// </summary>
    public Guid PipelineId { get; internal set; }

    /// <summary>
    ///     Unique run identifier for this pipeline execution.
    /// </summary>
    public Guid RunId { get; internal set; }

    /// <summary>
    ///     Logical pipeline name for this execution context.
    /// </summary>
    public string? PipelineName { get; internal set; }

    /// <summary>
    ///     The <see cref="PipelineId" /> of the pipeline that started this run as a sub-pipeline, or null for a
    ///     top-level run.
    /// </summary>
    /// <remarks>
    ///     Set by composite nodes (NPipeline.Extensions.Composition), which start one sub-pipeline run per item.
    /// </remarks>
    public Guid? ParentPipelineId { get; internal set; }

    /// <summary>
    ///     The <see cref="PipelineName" /> of the parent pipeline, or null for a top-level run or an unnamed parent.
    /// </summary>
    public string? ParentPipelineName { get; internal set; }

    /// <summary>
    ///     The id of the parent pipeline's node that started this run, or null for a top-level run.
    /// </summary>
    public string? ParentNodeId { get; internal set; }

    /// <summary>
    ///     Whether this run is a sub-pipeline started by a node of another pipeline.
    /// </summary>
    public bool IsNested => ParentPipelineId is not null;
}
