using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using NPipeline.ErrorHandling;
using NPipeline.Nodes.Internal;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     An abstract base class for creating a node that performs a keyed join on two input streams.
///     Every item is paired with every item on the other input that shares its key, so one-to-many and many-to-many
///     relationships produce one output per matching pair.
/// </summary>
/// <remarks>
///     This node is stateful: because a matching item can arrive on the other input at any time, items from both inputs are held
///     in memory until the input completes. To bound memory, configure <see cref="MaxCapacity" /> to limit the number of items
///     retained per input, or, when each key occurs at most once on each input, set <see cref="Cardinality" /> to
///     <see cref="JoinCardinality.OneToOne" /> so that items are released as soon as they match.
/// </remarks>
/// <typeparam name="TKey">The type of the key used for joining. Must be not-null.</typeparam>
/// <typeparam name="TIn1">The type of the data from the first input stream.</typeparam>
/// <typeparam name="TIn2">The type of the data from the second input stream.</typeparam>
/// <typeparam name="TOut">The type of the output data after the join.</typeparam>
public abstract class KeyedJoinNode<TKey, TIn1, TIn2, TOut> : BaseJoinNode<TKey, TIn1, TIn2, TOut> where TKey : notnull
{
    /// <summary>
    ///     Gets or sets the type of join to perform. Defaults to <see cref="JoinType.Inner" />.
    /// </summary>
    public JoinType JoinType { get; set; } = JoinType.Inner;

    /// <summary>
    ///     Gets or sets the maximum number of items retained for each input.
    ///     <c>null</c> indicates unlimited capacity (default). Set to a positive value to prevent unbounded memory growth.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Once an input reaches capacity, its new items are still joined with the items already retained from the other
    ///         input, but are not retained themselves, so they cannot match items that arrive later. If such an item matches
    ///         nothing and its side is preserved by the join type (for example, a left item in a left outer join), it is emitted
    ///         immediately as an unmatched item. Otherwise it is discarded.
    ///     </para>
    ///     <para>
    ///         Setting this to a reasonable value (e.g., 10000) can help prevent memory exhaustion when streams are large or
    ///         unbalanced, at the cost of missing matches once the limit is reached.
    ///     </para>
    /// </remarks>
    public int? MaxCapacity { get; set; }

    /// <summary>
    ///     Gets or sets how many times a key can match. Defaults to <see cref="JoinCardinality.ManyToMany" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         In <see cref="JoinCardinality.OneToOne" /> mode, an item that finds a match on the other input is joined with it and
    ///         neither item is retained, so memory holds only the items still waiting for a match, plus the keys that have matched.
    ///         Each key is joined once. A later item with a key that has matched, or a second item with the same key on the input
    ///         that is already waiting for a match, is a duplicate, handled by <see cref="DuplicateKeyPolicy" />; the first item to
    ///         arrive wins.
    ///     </para>
    ///     <para>
    ///         Null keys and <see cref="MaxCapacity" /> behave as in many-to-many mode.
    ///     </para>
    /// </remarks>
    public JoinCardinality Cardinality { get; set; } = JoinCardinality.ManyToMany;

    /// <summary>
    ///     Gets or sets what a one-to-one join does with an item whose key is a duplicate. Defaults to
    ///     <see cref="Nodes.DuplicateKeyPolicy.Drop" />.
    /// </summary>
    /// <remarks>
    ///     Applies only when <see cref="Cardinality" /> is <see cref="JoinCardinality.OneToOne" />. Setting another policy on a
    ///     many-to-many join fails the join when it starts.
    /// </remarks>
    public DuplicateKeyPolicy DuplicateKeyPolicy { get; set; } = DuplicateKeyPolicy.Drop;

    /// <summary>
    ///     Gets or sets the maximum number of matched keys a one-to-one join remembers for duplicate detection.
    ///     <c>null</c> indicates unlimited (default).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Every matched key is remembered so that a later item with the same key is recognised as a duplicate, so this set
    ///         grows with the number of distinct keys. Once the limit is reached, the oldest key is forgotten. A duplicate that
    ///         arrives after its key was forgotten is treated as a new item: it is retained, and may match or be emitted as
    ///         unmatched.
    ///     </para>
    ///     <para>
    ///         Applies only when <see cref="Cardinality" /> is <see cref="JoinCardinality.OneToOne" />. Setting it on a many-to-many
    ///         join fails the join when it starts. For unbounded streams, consider <see cref="TimeWindowedJoinNode{TKey, TIn1, TIn2, TOut}" />,
    ///         whose windows bound the retained state.
    ///     </para>
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The value is zero or negative.</exception>
    public int? MaxMatchedKeys
    {
        get;
        set
        {
            if (value is <= 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, $"{nameof(MaxMatchedKeys)} must be positive, or null for unlimited.");

            field = value;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    ///     In many-to-many mode, items from both inputs are retained for the lifetime of the stream so that each item is paired with
    ///     every item on the other side that shares its key, including items that arrive later. In one-to-one mode, an item is
    ///     retained only until it matches. When the input completes, outer joins emit the retained items that never matched.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    ///     One-to-one options are set on a many-to-many join (NP0427).
    /// </exception>
    /// <exception cref="DeadLetterSinkNotConfiguredException">
    ///     <see cref="DuplicateKeyPolicy" /> is <see cref="Nodes.DuplicateKeyPolicy.DeadLetter" /> and the context has no
    ///     dead-letter sink (NP0424).
    /// </exception>
    protected override IAsyncEnumerable<TOut> ExecuteJoinAsync(IAsyncEnumerable<object?> inputStream, PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Validated here rather than in the iterators, so a misconfigured join fails before it reads any item.
        if (Cardinality == JoinCardinality.ManyToMany)
        {
            if (DuplicateKeyPolicy != DuplicateKeyPolicy.Drop || MaxMatchedKeys is not null)
                throw new InvalidOperationException(ErrorMessages.JoinOptionsRequireOneToOne(ResolveNodeId(context), DescribeOneToOneOptions()));

            return ExecuteManyToManyAsync(inputStream, cancellationToken);
        }

        if (DuplicateKeyPolicy == DuplicateKeyPolicy.DeadLetter && context.DeadLetterSink is null)
            throw DeadLetterSinkNotConfiguredException.ForDuplicateJoinKeys(ResolveNodeId(context));

        return ExecuteOneToOneAsync(inputStream, context, cancellationToken);
    }

    private async IAsyncEnumerable<TOut> ExecuteManyToManyAsync(IAsyncEnumerable<object?> inputStream,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var left = new JoinSide<TKey, TIn1>();
        var right = new JoinSide<TKey, TIn2>();
        var emitUnmatchedLeft = JoinType is JoinType.LeftOuter or JoinType.FullOuter;
        var emitUnmatchedRight = JoinType is JoinType.RightOuter or JoinType.FullOuter;

        var (getKey1, getKey2) = ResolveKeySelectors();

        await foreach (var item in inputStream.WithCancellation(cancellationToken))
        {
            if (item is TIn1 item1)
            {
                var key = getKey1(item1);

                // Null keys never match. An outer join emits the row at once when its side is preserved.
                if (key is null)
                {
                    if (emitUnmatchedLeft)
                        yield return CreateOutputFromLeft(item1);

                    continue;
                }

                var matched = false;

                if (right.TryMatch(key, out var matches))
                {
                    matched = true;

                    for (var i = 0; i < matches.Count; i++)
                    {
                        yield return CreateOutput(item1, matches[i]);
                    }
                }

                if (CanRetain(left))
                    left.Add(key, item1, matched);
                else if (!matched && emitUnmatchedLeft)
                    yield return CreateOutputFromLeft(item1);
            }
            else if (item is TIn2 item2)
            {
                var key = getKey2(item2);

                if (key is null)
                {
                    if (emitUnmatchedRight)
                        yield return CreateOutputFromRight(item2);

                    continue;
                }

                var matched = false;

                if (left.TryMatch(key, out var matches))
                {
                    matched = true;

                    for (var i = 0; i < matches.Count; i++)
                    {
                        yield return CreateOutput(matches[i], item2);
                    }
                }

                if (CanRetain(right))
                    right.Add(key, item2, matched);
                else if (!matched && emitUnmatchedRight)
                    yield return CreateOutputFromRight(item2);
            }
        }

        // Handle unmatched items for outer joins at the end of the streams
        if (emitUnmatchedLeft)
        {
            foreach (var unmatchedLeft in left.Unmatched())
            {
                yield return CreateOutputFromLeft(unmatchedLeft);
            }
        }

        if (emitUnmatchedRight)
        {
            foreach (var unmatchedRight in right.Unmatched())
            {
                yield return CreateOutputFromRight(unmatchedRight);
            }
        }
    }

    private async IAsyncEnumerable<TOut> ExecuteOneToOneAsync(IAsyncEnumerable<object?> inputStream, PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var left = new JoinSide<TKey, TIn1>();
        var right = new JoinSide<TKey, TIn2>();
        var matchedKeys = new MatchedKeySet<TKey>(MaxMatchedKeys);
        var emitUnmatchedLeft = JoinType is JoinType.LeftOuter or JoinType.FullOuter;
        var emitUnmatchedRight = JoinType is JoinType.RightOuter or JoinType.FullOuter;
        var policy = DuplicateKeyPolicy;
        var duplicates = new DuplicateHandler(this, context, policy);

        var (getKey1, getKey2) = ResolveKeySelectors();

        await foreach (var item in inputStream.WithCancellation(cancellationToken))
        {
            if (item is TIn1 item1)
            {
                var key = getKey1(item1);

                if (key is null)
                {
                    if (emitUnmatchedLeft)
                        yield return CreateOutputFromLeft(item1);

                    continue;
                }

                // A key that has matched, or that this input is already waiting on, is a duplicate: the first arrival wins.
                if (matchedKeys.Contains(key) || left.ContainsKey(key))
                {
                    if (policy == DuplicateKeyPolicy.EmitAsUnmatched && emitUnmatchedLeft)
                        yield return CreateOutputFromLeft(item1);
                    else
                        await duplicates.HandleAsync(item1!, key, JoinInputSide.Left, cancellationToken).ConfigureAwait(false);

                    continue;
                }

                if (right.TryTake(key, out var match))
                {
                    matchedKeys.Add(key);
                    yield return CreateOutput(item1, match);
                }
                else if (CanRetain(left))
                    left.Add(key, item1, false);
                else if (emitUnmatchedLeft)
                    yield return CreateOutputFromLeft(item1);
            }
            else if (item is TIn2 item2)
            {
                var key = getKey2(item2);

                if (key is null)
                {
                    if (emitUnmatchedRight)
                        yield return CreateOutputFromRight(item2);

                    continue;
                }

                if (matchedKeys.Contains(key) || right.ContainsKey(key))
                {
                    if (policy == DuplicateKeyPolicy.EmitAsUnmatched && emitUnmatchedRight)
                        yield return CreateOutputFromRight(item2);
                    else
                        await duplicates.HandleAsync(item2!, key, JoinInputSide.Right, cancellationToken).ConfigureAwait(false);

                    continue;
                }

                if (left.TryTake(key, out var match))
                {
                    matchedKeys.Add(key);
                    yield return CreateOutput(match, item2);
                }
                else if (CanRetain(right))
                    right.Add(key, item2, false);
                else if (emitUnmatchedRight)
                    yield return CreateOutputFromRight(item2);
            }
        }

        // Every item still retained never matched.
        if (emitUnmatchedLeft)
        {
            foreach (var unmatchedLeft in left.Unmatched())
            {
                yield return CreateOutputFromLeft(unmatchedLeft);
            }
        }

        if (emitUnmatchedRight)
        {
            foreach (var unmatchedRight in right.Unmatched())
            {
                yield return CreateOutputFromRight(unmatchedRight);
            }
        }
    }

    /// <summary>
    ///     Returns the item sent to the dead-letter sink for a duplicate input item.
    /// </summary>
    private protected virtual object ToDeadLetterItem(object item) => item;

    private string ResolveNodeId(PipelineContext context) =>
        context.NodeEnvironment.TryGetNodeId(this, out var nodeId)
            ? nodeId
            : GetType().Name;

    private string DescribeOneToOneOptions()
    {
        var options = new List<string>(2);

        if (DuplicateKeyPolicy != DuplicateKeyPolicy.Drop)
            options.Add($"{nameof(DuplicateKeyPolicy)} = {DuplicateKeyPolicy}");

        if (MaxMatchedKeys is not null)
            options.Add($"{nameof(MaxMatchedKeys)} = {MaxMatchedKeys}");

        return string.Join(" and ", options);
    }

    /// <summary>
    ///     Resolves the key selectors used to extract join keys from each input.
    /// </summary>
    private protected virtual (Func<TIn1, TKey> GetKey1, Func<TIn2, TKey> GetKey2) ResolveKeySelectors() => GetKeySelectors();

    /// <summary>
    ///     Determines whether another item can be retained on the specified side based on capacity constraints.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CanRetain<T>(JoinSide<TKey, T> side)
    {
        if (MaxCapacity is null)
            return true;

        return side.Count < MaxCapacity;
    }

    /// <summary>
    ///     Drops or dead-letters the duplicates of a one-to-one join run, logging each one at debug level.
    /// </summary>
    private sealed class DuplicateHandler(KeyedJoinNode<TKey, TIn1, TIn2, TOut> node, PipelineContext context, DuplicateKeyPolicy policy)
    {
        private string? _nodeId;
        private ILogger? _logger;

        private string NodeId => _nodeId ??= node.ResolveNodeId(context);

        public async ValueTask HandleAsync(object item, TKey key, JoinInputSide side, CancellationToken cancellationToken)
        {
            _logger ??= context.Observability.LoggerFactory.CreateLogger(node.GetType().FullName ?? node.GetType().Name);

            // EmitAsUnmatched on an input the join type does not preserve discards the item, like Drop.
            var applied = policy == DuplicateKeyPolicy.DeadLetter
                ? DuplicateKeyPolicy.DeadLetter
                : DuplicateKeyPolicy.Drop;

            if (_logger.IsEnabled(LogLevel.Debug))
                KeyedJoinNodeLogMessages.DuplicateKey(_logger, NodeId, side == JoinInputSide.Left ? "left" : "right", applied.ToString());

            if (applied != DuplicateKeyPolicy.DeadLetter)
                return;

            var error = new DuplicateJoinKeyException(NodeId, key, side);
            var attribution = FailureAttributionResolver.Resolve(error, context, NodeId, 0);
            var envelope = new DeadLetterEnvelope(node.ToDeadLetterItem(item), error, attribution);

            // Checked non-null when the join started.
            await context.DeadLetterSink!.HandleAsync(envelope, context, cancellationToken).ConfigureAwait(false);
        }
    }
}
