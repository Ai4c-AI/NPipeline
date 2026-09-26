namespace NPipeline.Nodes.Internal;

/// <summary>
///     The keys a one-to-one join has already matched, remembered so that later items with the same key are recognised as
///     duplicates.
/// </summary>
/// <remarks>
///     With a capacity, the oldest key is forgotten once the set is full. A duplicate of a forgotten key is then treated as a
///     new item.
/// </remarks>
/// <typeparam name="TKey">The join key type.</typeparam>
internal sealed class MatchedKeySet<TKey>(int? capacity) where TKey : notnull
{
    private readonly HashSet<TKey> _keys = [];
    private readonly Queue<TKey>? _order = capacity is null ? null : new Queue<TKey>();

    public int Count => _keys.Count;

    public bool Contains(TKey key) => _keys.Contains(key);

    /// <summary>
    ///     Records <paramref name="key" /> as matched, forgetting the oldest key when the set is at capacity.
    /// </summary>
    /// <remarks>The caller only adds keys that are not already in the set.</remarks>
    public void Add(TKey key)
    {
        if (_order is not null)
        {
            if (_keys.Count >= capacity)
                _keys.Remove(_order.Dequeue());

            _order.Enqueue(key);
        }

        _keys.Add(key);
    }
}
