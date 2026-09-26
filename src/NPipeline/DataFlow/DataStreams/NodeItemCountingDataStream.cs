using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.Execution;
using NPipeline.Observability;

namespace NPipeline.DataFlow.DataStreams;

/// <summary>
///     Which item count a <see cref="NodeItemCountingDataStream{T}" /> records.
/// </summary>
internal enum NodeItemCount
{
    /// <summary>Each item is counted as processed: the stream is the node's input.</summary>
    Processed,

    /// <summary>Each item is counted as emitted: the stream is the node's output.</summary>
    Emitted,
}

/// <summary>
///     Counts the items that pass through a node's input or output into the node's observability scope.
/// </summary>
/// <remarks>
///     <para>
///         Transform strategies count their items themselves. Sources, joins, aggregates and sinks run no strategy, so
///         the executor wraps their streams with this instead. It wraps at the node boundary, before any fan-out, so an item
///         a branch delivers to several consumers is counted once.
///     </para>
///     <para>
///         When the stream owns the scope, the scope is disposed once the stream has been read to the end, has failed or
///         has been abandoned, which ends the node's observation with its dataflow rather than at the end of the run. A
///         read that fails records the failure on the scope first.
///     </para>
/// </remarks>
internal static class NodeItemCounting
{
    private static readonly ConcurrentDictionary<Type, Func<IDataStream, IAutoObservabilityScope, NodeItemCount, bool, IDataStream>> Wrappers = new();

    private static readonly MethodInfo WrapGenericMethod =
        typeof(NodeItemCounting).GetMethod(nameof(WrapGeneric), BindingFlags.NonPublic | BindingFlags.Static)!;

    /// <summary>
    ///     Wraps <paramref name="stream" /> so each item it yields is counted on <paramref name="scope" />.
    /// </summary>
    /// <param name="stream">The node's input or output stream.</param>
    /// <param name="scope">The node's observability scope. The null scope returns the stream unwrapped.</param>
    /// <param name="count">Whether items are counted as processed or emitted.</param>
    /// <param name="ownsScope">Whether the stream disposes the scope once it is no longer read.</param>
    public static IDataStream Wrap(IDataStream stream, IAutoObservabilityScope scope, NodeItemCount count, bool ownsScope)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(scope);

        if (ReferenceEquals(scope, NodeExecutionScopeRegistry.NullScope))
            return stream;

        var wrap = Wrappers.GetOrAdd(stream.GetDataType(),
            static type => WrapGenericMethod.MakeGenericMethod(type)
                .CreateDelegate<Func<IDataStream, IAutoObservabilityScope, NodeItemCount, bool, IDataStream>>());

        return wrap(stream, scope, count, ownsScope);
    }

    private static IDataStream WrapGeneric<T>(IDataStream stream, IAutoObservabilityScope scope, NodeItemCount count, bool ownsScope) =>
        new NodeItemCountingDataStream<T>((IDataStream<T>)stream, scope, count, ownsScope);
}

/// <summary>
///     A pass-through stream that counts each item it yields on a node's observability scope. Created through
///     <see cref="NodeItemCounting.Wrap" />.
/// </summary>
/// <typeparam name="T">The type of the items in the stream.</typeparam>
internal sealed class NodeItemCountingDataStream<T> : IForwardOnlyDataStream<T>
{
    private readonly NodeItemCount _count;
    private readonly IDataStream<T> _inner;
    private readonly bool _ownsScope;
    private readonly IAutoObservabilityScope _scope;
    private bool _disposed;

    public NodeItemCountingDataStream(IDataStream<T> inner, IAutoObservabilityScope scope, NodeItemCount count, bool ownsScope)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(scope);
        _inner = inner;
        _scope = scope;
        _count = count;
        _ownsScope = ownsScope;
    }

    public string StreamName => _inner.StreamName;

    public Type GetDataType() => typeof(T);

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return EnumerateWithCounting(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await foreach (var item in EnumerateWithCounting(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        // A stream that is never read still releases its scope, so the node's observation does not wait for the run.
        if (_ownsScope)
            _scope.Dispose();

        await _inner.DisposeAsync().ConfigureAwait(false);
    }

    private async IAsyncEnumerable<T> EnumerateWithCounting([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        try
        {
#pragma warning disable CA2007

            // CA2007 false positive: the enumerator comes from a ConfigureAwait(false) sequence, so its
            // MoveNextAsync and DisposeAsync already return configured awaitables - the analyzer only
            // recognises ConfigureAwait applied directly to the await using expression.
            await using var enumerator = _inner.WithCancellation(cancellationToken).ConfigureAwait(false).GetAsyncEnumerator();
#pragma warning restore CA2007

            while (true)
            {
                try
                {
                    if (!await enumerator.MoveNextAsync())
                        break;
                }
                catch (Exception ex) when (_ownsScope)
                {
                    _scope.RecordFailure(ex);
                    throw;
                }

                if (_count == NodeItemCount.Emitted)
                    _scope.IncrementEmitted();
                else
                    _scope.IncrementProcessed();

                yield return enumerator.Current;
            }
        }
        finally
        {
            // Runs on normal completion, on an abandoned enumeration and on a thrown exception. The scope's dispose is
            // idempotent, so a later DisposeAsync does not release it twice.
            if (_ownsScope)
                _scope.Dispose();
        }
    }
}
