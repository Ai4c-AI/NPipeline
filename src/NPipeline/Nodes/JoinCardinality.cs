namespace NPipeline.Nodes;

/// <summary>
///     Specifies how many times a key can match in a <see cref="KeyedJoinNode{TKey, TIn1, TIn2, TOut}" />.
/// </summary>
public enum JoinCardinality
{
    /// <summary>
    ///     Every item pairs with every item on the other input that shares its key. Items from both inputs are retained until the
    ///     input completes.
    /// </summary>
    ManyToMany = 0,

    /// <summary>
    ///     Each key matches at most once. Once an item matches, both items are released, and later items with that key are handled
    ///     by <see cref="DuplicateKeyPolicy" />.
    /// </summary>
    OneToOne = 1,
}
