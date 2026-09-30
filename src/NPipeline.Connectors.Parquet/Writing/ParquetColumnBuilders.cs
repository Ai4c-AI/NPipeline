using System.Buffers;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Parquet.Reading;
using NPipeline.Connectors.Parquet.Schema;
using Parquet;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet.Writing;

/// <summary>
///     Collects one column's values for the row group being written, in a pooled array of the column's storage type, and
///     writes them with Parquet.Net's typed overloads.
/// </summary>
internal abstract class ParquetColumnBuilder(Field field) : IDisposable
{
    public Field Field { get; } = field;

    /// <summary>An estimate of the values' size before compression, for sizing row groups.</summary>
    public long EstimatedBytes { get; protected set; }

    public abstract Task WriteAsync(ParquetRowGroupWriter writer, CancellationToken cancellationToken);

    /// <summary>Empties the builder for the next row group.</summary>
    public abstract void Reset();

    public abstract void Dispose();

    protected static DataField Leaf(Field field) => field as DataField ?? (DataField)((ListField)field).Item;
}

/// <summary>A builder that takes values of a member's type.</summary>
internal abstract class ParquetColumnBuilder<TValue>(Field field) : ParquetColumnBuilder(field)
{
    public abstract void Add(TValue value);
}

/// <summary>A growable pooled array.</summary>
internal struct PooledBuffer<T>
{
    private T[] _items;

    public PooledBuffer()
    {
        _items = [];
        Count = 0;
    }

    public int Count { get; private set; }

    public ReadOnlyMemory<T> Memory => _items.AsMemory(0, Count);

    public ArraySegment<T> Segment => new(_items, 0, Count);

    public void Add(T item)
    {
        if (Count == _items.Length)
        {
            var larger = ArrayPool<T>.Shared.Rent(Math.Max(Count * 2, 1024));
            _items.AsSpan(0, Count).CopyTo(larger);
            Return();
            _items = larger;
        }

        _items[Count++] = item;
    }

    public void Reset()
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _items.AsSpan(0, Count).Clear();

        Count = 0;
    }

    public void Return()
    {
        if (_items.Length > 0)
            ArrayPool<T>.Shared.Return(_items, RuntimeHelpers.IsReferenceOrContainsReferences<T>());

        _items = [];
    }
}

/// <summary>A scalar column: each value is converted to a cell of the array the column is written from.</summary>
internal sealed class ScalarBuilder<TValue, TCell>(Field field, Func<TValue, TCell> convert, Func<ParquetRowGroupWriter, DataField, PooledBuffer<TCell>, CancellationToken, Task> write)
    : ParquetColumnBuilder<TValue>(field)
{
    private static readonly int CellSize = typeof(TCell).IsValueType ? Unsafe.SizeOf<TCell>() : 8;

    private PooledBuffer<TCell> _cells = new();

    public override void Add(TValue value)
    {
        var cell = convert(value);
        _cells.Add(cell);

        EstimatedBytes += cell switch
        {
            string text => text.Length,
            byte[] bytes => bytes.Length,
            _ => CellSize,
        };
    }

    public override Task WriteAsync(ParquetRowGroupWriter writer, CancellationToken cancellationToken) => write(writer, Leaf(Field), _cells, cancellationToken);

    public override void Reset()
    {
        _cells.Reset();
        EstimatedBytes = 0;
    }

    public override void Dispose() => _cells.Return();
}

/// <summary>
///     A list column, written through the raw API with its levels: a null list is below the list's level, an empty list at
///     it, a null element one below the maximum and a value at the maximum; a row's first entry repeats at 0, the rest at 1.
/// </summary>
internal sealed class ListBuilder<TValue, TElement, TRaw>(Field field, Func<TElement, TRaw> convert) : ParquetColumnBuilder<TValue>(field)
    where TRaw : struct
{
    private readonly int _maxDefinition = Leaf(field).MaxDefinitionLevel;
    private readonly bool _elementNullable = Leaf(field).IsNullable;
    private PooledBuffer<int> _definitions = new();
    private PooledBuffer<int> _repetitions = new();
    private PooledBuffer<TRaw> _values = new();

    private int ListLevel => _maxDefinition - 1 - (_elementNullable ? 1 : 0);

    public override void Add(TValue value)
    {
        // A ParquetRow's boxed list of value types (an int?[], say) is not an IEnumerable<object?>: arrays are covariant
        // only over reference types, so the boxed path enumerates it untyped.
        var items = value switch
        {
            IEnumerable<TElement> typed => typed,
            System.Collections.IEnumerable untyped when typeof(TElement) == typeof(object) => untyped.Cast<TElement>(),
            _ => null,
        };

        if (items is null)
        {
            Level(ListLevel - 1, 0);
            return;
        }

        var repetition = 0;

        foreach (var item in items)
        {
            if (item is null)
            {
                if (!_elementNullable)
                    throw new InvalidOperationException($"List column '{Field.Name}' cannot hold null elements.");

                Level(_maxDefinition - 1, repetition);
            }
            else
            {
                var raw = convert(item);
                _values.Add(raw);
                Level(_maxDefinition, repetition);

                EstimatedBytes += raw switch
                {
                    ReadOnlyMemory<char> text => text.Length,
                    ReadOnlyMemory<byte> bytes => bytes.Length,
                    _ => Unsafe.SizeOf<TRaw>(),
                };
            }

            repetition = 1;
        }

        // An empty list.
        if (repetition == 0)
            Level(ListLevel, 0);
    }

    public override Task WriteAsync(ParquetRowGroupWriter writer, CancellationToken cancellationToken) =>
        writer.WriteAllPartsAsync<TRaw>(Leaf(Field), _values.Memory, _definitions.Memory, _repetitions.Memory, cancellationToken);

    public override void Reset()
    {
        _values.Reset();
        _definitions.Reset();
        _repetitions.Reset();
        EstimatedBytes = 0;
    }

    public override void Dispose()
    {
        _values.Return();
        _definitions.Return();
        _repetitions.Return();
    }

    private void Level(int definition, int repetition)
    {
        _definitions.Add(definition);
        _repetitions.Add(repetition);
        EstimatedBytes += 8;
    }
}

/// <summary>Creates builders: for a member type (the attribute mapping), or for a column's storage type (writing <see cref="ParquetRow" />s).</summary>
internal static class ParquetColumnBuilders
{
    private static readonly MethodInfo ScalarMethod = Method(nameof(Scalar));
    private static readonly MethodInfo ListMethod = Method(nameof(List));

    /// <summary>A builder that takes <paramref name="memberType" /> values for <paramref name="field" />, as the schema for that member type declares.</summary>
    public static ParquetColumnBuilder ForMember(Field field, Type memberType)
    {
        if (ParquetTypeMap.ListElement(memberType) is { } element)
        {
            var underlying = Nullable.GetUnderlyingType(element) ?? element;
            return (ParquetColumnBuilder)ListMethod.MakeGenericMethod(memberType, element, RawType(underlying)).Invoke(null, [field])!;
        }

        return (ParquetColumnBuilder)ScalarMethod.MakeGenericMethod(memberType, CellType(Leaf(field), memberType)).Invoke(null, [field])!;
    }

    /// <summary>A builder that takes boxed storage values (as a <see cref="ParquetRow" /> holds them) for any column of a file's schema.</summary>
    public static ParquetColumnBuilder<object?> ForStorage(Field field)
    {
        var leaf = Leaf(field);
        var storage = leaf is TimeDataField ? typeof(TimeOnly) : ParquetTypeMap.ClrType(leaf);

        if (field is ListField)
        {
            var rawType = leaf is TimeDataField ? leaf.ClrType : RawType(storage);
            var method = Method(nameof(BoxedList)).MakeGenericMethod(storage, rawType);
            return (ParquetColumnBuilder<object?>)method.Invoke(null, [field])!;
        }

        var cell = leaf is TimeDataField ? typeof(Nullable<>).MakeGenericType(leaf.ClrType) : CellType(leaf, storage, leaf.IsNullable);
        return (ParquetColumnBuilder<object?>)Method(nameof(BoxedScalar)).MakeGenericMethod(storage, cell).Invoke(null, [field])!;
    }

    private static MethodInfo Method(string name) => typeof(ParquetColumnBuilders).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)!;

    private static DataField Leaf(Field field) => field as DataField ?? (DataField)((ListField)field).Item;

    private static Type CellType(DataField leaf, Type memberType, bool? nullable = null)
    {
        var storage = leaf is TimeDataField ? leaf.ClrType : ParquetTypeMap.StorageType(memberType);

        return storage.IsValueType && (nullable ?? leaf.IsNullable)
            ? typeof(Nullable<>).MakeGenericType(storage)
            : storage;
    }

    /// <summary>The struct a list's elements are written as: text and bytes as memory, the rest as their storage type.</summary>
    private static Type RawType(Type underlying) =>
        ParquetTypeMap.StorageType(underlying) switch
        {
            var s when s == typeof(string) => typeof(ReadOnlyMemory<char>),
            var s when s == typeof(byte[]) => typeof(ReadOnlyMemory<byte>),
            var s => s,
        };

    private static ParquetColumnBuilder Scalar<TValue, TCell>(Field field) =>
        new ScalarBuilder<TValue, TCell>(field, CellConverter<TValue, TCell>(), Writer<TCell>());

    private static ParquetColumnBuilder List<TValue, TElement, TRaw>(Field field)
        where TRaw : struct =>
        new ListBuilder<TValue, TElement, TRaw>(field, RawConverter<TElement, TRaw>());

    private static ParquetColumnBuilder<object?> BoxedScalar<TStorage, TCell>(Field field)
    {
        var convert = CellConverter<TStorage, TCell>();
        return new ScalarBuilder<object?, TCell>(field, value => value is null ? default! : convert((TStorage)value), Writer<TCell>());
    }

    private static ParquetColumnBuilder<object?> BoxedList<TStorage, TRaw>(Field field)
        where TRaw : struct
    {
        var convert = RawConverter<TStorage, TRaw>();

        // A ParquetRow holds a list as an array of nullable storage values.
        return new ListBuilder<object?, object?, TRaw>(field, value => convert((TStorage)value!));
    }

    /// <summary>Member (or storage) value to the cell written: TCell is the storage type, its nullable form, or a reference type.</summary>
    private static Func<TValue, TCell> CellConverter<TValue, TCell>()
    {
        var underlying = Nullable.GetUnderlyingType(typeof(TValue));
        var cellUnderlying = Nullable.GetUnderlyingType(typeof(TCell)) ?? typeof(TCell);
        var toStorage = typeof(ParquetColumnBuilders).GetMethod(nameof(ToStorage), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(underlying ?? typeof(TValue), cellUnderlying).Invoke(null, null)!;

        var compose = typeof(ParquetColumnBuilders).GetMethod(
                underlying is null ? typeof(TCell) != cellUnderlying ? nameof(ToNullable) : nameof(Direct)
                : typeof(TCell).IsValueType ? nameof(FromNullable) : nameof(FromNullableToReference),
                BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(underlying ?? typeof(TValue), cellUnderlying);

        return (Func<TValue, TCell>)compose.Invoke(null, [toStorage])!;
    }

    private static Func<TElement, TRaw> RawConverter<TElement, TRaw>()
        where TRaw : struct
    {
        var underlying = Nullable.GetUnderlyingType(typeof(TElement)) ?? typeof(TElement);

        if (typeof(TRaw) == typeof(ReadOnlyMemory<char>))
        {
            var format = typeof(ParquetColumnBuilders).GetMethod(nameof(ToStorage), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(underlying, typeof(string)).Invoke(null, null)!;

            return (Func<TElement, TRaw>)typeof(ParquetColumnBuilders).GetMethod(nameof(TextRaw), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(typeof(TElement), underlying).Invoke(null, [format])!;
        }

        if (typeof(TRaw) == typeof(ReadOnlyMemory<byte>))
            return (Func<TElement, TRaw>)(object)(Func<TElement, ReadOnlyMemory<byte>>)(static value => (byte[])(object)value!);

        var storage = typeof(ParquetColumnBuilders).GetMethod(nameof(ToStorage), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(underlying, typeof(TRaw)).Invoke(null, null)!;

        return (Func<TElement, TRaw>)typeof(ParquetColumnBuilders).GetMethod(nameof(ElementRaw), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(TElement), underlying, typeof(TRaw)).Invoke(null, [storage])!;
    }

    private static Func<TElement, ReadOnlyMemory<char>> TextRaw<TElement, TUnderlying>(Func<TUnderlying, string> format) =>
        value => format((TUnderlying)(object)value!).AsMemory();

    private static Func<TElement, TRaw> ElementRaw<TElement, TUnderlying, TRaw>(Func<TUnderlying, TRaw> storage) =>
        typeof(TElement) == typeof(TUnderlying)
            ? (Func<TElement, TRaw>)(object)storage
            : value => storage((TUnderlying)(object)value!);

    private static Func<TValue, TCell> Direct<TValue, TCell>(Func<TValue, TCell> toStorage) => toStorage;

    private static Func<TValue?, TCell?> FromNullable<TValue, TCell>(Func<TValue, TCell> toStorage)
        where TValue : struct
        where TCell : struct =>
        value => value is { } v ? toStorage(v) : null;

    private static Func<TValue?, TCell?> FromNullableToReference<TValue, TCell>(Func<TValue, TCell> toStorage)
        where TValue : struct
        where TCell : class =>
        value => value is { } v ? toStorage(v) : null;

    private static Func<TValue, TCell?> ToNullable<TValue, TCell>(Func<TValue, TCell> toStorage)
        where TCell : struct =>
        value => value is null ? null : toStorage(value);

    /// <summary>A non-null value (member or storage type) to its storage type.</summary>
    private static Func<TFrom, TStorage> ToStorage<TFrom, TStorage>()
    {
        var from = typeof(TFrom);
        var to = typeof(TStorage);

        if (from == to)
        {
            if (from == typeof(DateTime))
                return (Func<TFrom, TStorage>)(object)(Func<DateTime, DateTime>)(static value => value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : DateTime.SpecifyKind(value, DateTimeKind.Utc));

            return static value => (TStorage)(object)value!;
        }

        Delegate? convert = (from, to) switch
        {
            _ when to == typeof(string) => (Func<TFrom, string>)(static value => ScalarFormatter.Format(value)!),
            _ when from == typeof(DateTimeOffset) => (Func<DateTimeOffset, DateTime>)(static value => value.UtcDateTime),
            _ when from == typeof(TimeOnly) && to == typeof(long) => (Func<TimeOnly, long>)(static value => StorageNormalization.Time(value)),
            _ when from == typeof(TimeOnly) && to == typeof(int) => (Func<TimeOnly, int>)(static value => checked((int)(value.Ticks / TimeSpan.TicksPerMillisecond))),
            _ when from == typeof(TimeSpan) && to == typeof(long) => (Func<TimeSpan, long>)(static value => value.Ticks),
            _ => null,
        };

        return convert as Func<TFrom, TStorage>
               ?? throw new NotSupportedException($"{from.Name} values cannot be written to a {to.Name} column.");
    }

    private static Func<ParquetRowGroupWriter, DataField, PooledBuffer<TCell>, CancellationToken, Task> Writer<TCell>()
    {
        var cell = typeof(TCell);

        if (cell == typeof(string))
            return (Func<ParquetRowGroupWriter, DataField, PooledBuffer<TCell>, CancellationToken, Task>)(object)(Func<ParquetRowGroupWriter, DataField, PooledBuffer<string>, CancellationToken, Task>)
                (static (writer, field, cells, _) => writer.WriteAsync(field, cells.Segment));

        if (cell == typeof(byte[]))
            return (Func<ParquetRowGroupWriter, DataField, PooledBuffer<TCell>, CancellationToken, Task>)(object)(Func<ParquetRowGroupWriter, DataField, PooledBuffer<byte[]>, CancellationToken, Task>)
                (static (writer, field, cells, _) => writer.WriteAsync(field, cells.Segment));

        var method = typeof(ParquetColumnBuilders).GetMethod(
                Nullable.GetUnderlyingType(cell) is not null ? nameof(WriteNullable) : nameof(WriteRequired), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(Nullable.GetUnderlyingType(cell) ?? cell);

        return (Func<ParquetRowGroupWriter, DataField, PooledBuffer<TCell>, CancellationToken, Task>)method.Invoke(null, null)!;
    }

    private static Func<ParquetRowGroupWriter, DataField, PooledBuffer<T>, CancellationToken, Task> WriteRequired<T>()
        where T : struct =>
        static (writer, field, cells, cancellationToken) => writer.WriteAsync<T>(field, cells.Memory, null, null, cancellationToken);

    private static Func<ParquetRowGroupWriter, DataField, PooledBuffer<T?>, CancellationToken, Task> WriteNullable<T>()
        where T : struct =>
        static (writer, field, cells, cancellationToken) => writer.WriteAsync<T>(field, cells.Memory, null, null, cancellationToken);
}
