using NPipeline.Pipeline;

namespace NPipeline.ErrorHandling;

/// <summary>
///     Dead-letters what a source or sink node could not handle as an item: a file row that fails to map, or a request
///     an API rejected. Obtain one from <see cref="Nodes.SourceNode{TOut}.OpenDeadLetterChannel" /> inside
///     <c>OpenStream</c>, or <see cref="Nodes.SinkNode{TIn}.OpenDeadLetterChannel" /> inside <c>ConsumeAsync</c>; either
///     captures the node's id for the failure attribution.
/// </summary>
public sealed class DeadLetterChannel
{
    private readonly PipelineContext _context;

    internal DeadLetterChannel(PipelineContext context, string nodeId)
    {
        _context = context;
        NodeId = nodeId;
    }

    /// <summary>The id of the node the failures are attributed to.</summary>
    public string NodeId { get; }

    /// <summary>
    ///     Sends <paramref name="item" /> to the pipeline's dead-letter sink, attributed to <see cref="NodeId" />.
    /// </summary>
    /// <param name="item">What failed. There is often no typed item (a record that failed to map), so this is usually a description of it.</param>
    /// <param name="error">Why it failed.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <exception cref="DeadLetterSinkNotConfiguredException">The pipeline has no dead-letter sink.</exception>
    public Task SendAsync(object item, Exception error, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(error);

        var sink = _context.DeadLetterSink ?? throw new DeadLetterSinkNotConfiguredException(NodeId, error);
        var attribution = FailureAttributionResolver.Resolve(error, _context, NodeId, 0);
        return sink.HandleAsync(new DeadLetterEnvelope(item, error, attribution), _context, cancellationToken);
    }
}
