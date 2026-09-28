namespace NPipeline.Observability;

/// <summary>
///     A node's item counts so far.
/// </summary>
/// <param name="Processed">Items the node has processed, not counting items a node restart replayed.</param>
/// <param name="Emitted">Items the node has emitted.</param>
/// <param name="Replayed">Items a node restart read again after they had already been processed once.</param>
public readonly record struct NodeItemCounts(long Processed, long Emitted, long Replayed)
{
    /// <summary>
    ///     No items counted.
    /// </summary>
    public static NodeItemCounts Empty { get; } = new(0, 0, 0);
}
