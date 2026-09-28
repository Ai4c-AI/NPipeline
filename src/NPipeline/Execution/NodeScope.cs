namespace NPipeline.Execution;

/// <summary>
///     Carries the id of the node the runtime is running: a source while its <c>OpenStream</c> is called, a sink while its
///     <c>ConsumeAsync</c> runs. Neither has another way to learn its id, and both need it to attribute what they
///     dead-letter (see <see cref="Nodes.SourceNode{TOut}.OpenDeadLetterChannel" /> and
///     <see cref="Nodes.SinkNode{TIn}.OpenDeadLetterChannel" />).
/// </summary>
internal static class NodeScope
{
    private static readonly AsyncLocal<string?> Current = new();

    /// <summary>The id of the node being run, or <c>null</c> outside the runtime's call.</summary>
    public static string? CurrentNodeId => Current.Value;

    /// <summary>Sets <paramref name="nodeId" /> as the current node until the returned scope is disposed.</summary>
    public static Scope Enter(string nodeId)
    {
        var previous = Current.Value;
        Current.Value = nodeId;
        return new Scope(previous);
    }

    public readonly struct Scope(string? previous) : IDisposable
    {
        public void Dispose() => Current.Value = previous;
    }
}
