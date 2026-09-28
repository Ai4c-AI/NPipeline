using NPipeline.Pipeline;

namespace NPipeline.ErrorHandling;

/// <summary>
///     Dead-letters records that a source node cannot turn into items, such as a file row that fails to map. Obtain one
///     from <see cref="Nodes.SourceNode{TOut}.OpenDeadLetterChannel" /> inside <c>OpenStream</c>, which captures the
///     node's id for the failure attribution.
/// </summary>
public sealed class SourceDeadLetterChannel
{
    private readonly PipelineContext _context;

    internal SourceDeadLetterChannel(PipelineContext context, string nodeId)
    {
        _context = context;
        NodeId = nodeId;
    }

    /// <summary>The id of the source node the records are attributed to.</summary>
    public string NodeId { get; }

    /// <summary>
    ///     Sends <paramref name="item" /> to the pipeline's dead-letter sink, attributed to <see cref="NodeId" />.
    /// </summary>
    /// <param name="item">What failed. There is no typed item when a record fails to map, so this is usually a description of the record.</param>
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
