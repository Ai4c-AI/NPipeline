namespace NPipeline.Nodes;

/// <summary>
///     Identifies one of the two inputs of a join.
/// </summary>
public enum JoinInputSide
{
    /// <summary>
    ///     The first input (<c>TIn1</c>).
    /// </summary>
    Left = 0,

    /// <summary>
    ///     The second input (<c>TIn2</c>).
    /// </summary>
    Right = 1,
}
