namespace NPipeline.Execution;

/// <summary>
///     Carries the id of the source node whose <c>OpenStream</c> the runtime is calling. A source node has no other way to
///     learn its id, and needs it to attribute records it dead-letters (see
///     <see cref="Nodes.SourceNode{TOut}.OpenDeadLetterChannel" />).
/// </summary>
internal static class SourceNodeScope
{
    private static readonly AsyncLocal<string?> Current = new();

    /// <summary>The id of the source node being opened, or <c>null</c> outside the runtime's call to <c>OpenStream</c>.</summary>
    public static string? CurrentNodeId => Current.Value;

    /// <summary>Sets <paramref name="nodeId" /> as the current source node until the returned scope is disposed.</summary>
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
