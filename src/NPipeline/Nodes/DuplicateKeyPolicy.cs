namespace NPipeline.Nodes;

/// <summary>
///     Specifies what a one-to-one join does with an item whose key has already matched, or is already waiting for a match on
///     the same input.
/// </summary>
/// <seealso cref="JoinCardinality.OneToOne" />
public enum DuplicateKeyPolicy
{
    /// <summary>
    ///     Discard the duplicate.
    /// </summary>
    Drop = 0,

    /// <summary>
    ///     Send the duplicate to the pipeline's dead-letter sink, with a <see cref="ErrorHandling.DuplicateJoinKeyException" /> as
    ///     its error. The join fails before reading any item if no dead-letter sink is configured.
    /// </summary>
    DeadLetter = 1,

    /// <summary>
    ///     Emit the duplicate as an unmatched item if the join type preserves its input; otherwise discard it.
    /// </summary>
    EmitAsUnmatched = 2,
}
