using System.Collections.Concurrent;
using System.Reflection;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Parquet.Schema;
using Parquet;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet.Reading;

/// <summary>
///     One column of a file, reloaded with each row group. Its values stay in typed arrays; a record mapper reads them
///     through <see cref="Getter{TValue}" />, created once per file and member type, so reading a value neither boxes nor
///     looks anything up.
/// </summary>
internal abstract class ParquetColumn(Field field)
{
    private static readonly ConcurrentDictionary<(Type Clr, bool Nullable, bool List, int TimeUnit), Func<Field, ParquetColumn>> Factories = new();

    public Field Field { get; } = field;

    public string Name => Field.Name;

    public int RowCount { get; protected set; }

    /// <summary>A column for <paramref name="field" />, or <c>null</c> when this connector cannot read it (a struct or a map).</summary>
    public static ParquetColumn? Create(Field field)
    {
        var (leaf, isList) = field switch
        {
            DataField data => (data, false),
            ListField { Item: DataField item } => (item, true),
            _ => ((DataField?)null, false),
        };

        if (leaf is null)
            return null;

        var timeUnit = leaf is TimeDataField time ? (int)time.Precision + 1 : 0;
        var factory = Factories.GetOrAdd((ParquetTypeMap.ClrType(leaf), leaf.IsNullable, isList, timeUnit), static key => BuildFactory(key.Clr, key.Nullable, key.List, key.TimeUnit));
        return factory(field);
    }

    public abstract Task LoadAsync(ParquetRowGroupReader reader, CancellationToken cancellationToken);

    public abstract Func<int, TValue> Getter<TValue>();

    /// <summary>The value at <paramref name="row" /> in its storage type (a list as an array), for a <see cref="ParquetRow" />.</summary>
    public abstract object? Boxed(int row);

    protected static TValue Missing<TValue>() =>
        default(TValue) is null ? default! : throw new FieldConversionException(typeof(TValue), null, "the value is missing");

    protected static DataField Leaf(Field field) => field as DataField ?? (DataField)((ListField)field).Item;

    private static Func<Field, ParquetColumn> BuildFactory(Type clr, bool nullable, bool isList, int timeUnit)
    {
        // TIME columns hold integers in a unit; they are read as integers and exposed as TimeOnly.
        if (timeUnit > 0)
        {
            var unit = (TimeUnitPrecision)(timeUnit - 1);
            return isList ? f => ListColumns.Time(f, unit) : f => new TimeColumn(f, unit);
        }

        if (isList)
            return ListColumns.For(clr);

        if (clr == typeof(string))
            return static f => new StringColumn(f);

        if (clr == typeof(byte[]))
            return static f => new BinaryColumn(f);

        var type = (nullable ? typeof(OptionalValueColumn<>) : typeof(RequiredValueColumn<>)).MakeGenericType(clr);
        var constructor = type.GetConstructor([typeof(Field)])!;
        return f => (ParquetColumn)constructor.Invoke([f]);
    }
}

/// <summary>A non-nullable value column.</summary>
internal sealed class RequiredValueColumn<TStorage>(Field field) : ParquetColumn(field)
    where TStorage : struct
{
    private TStorage[] _values = [];

    public override async Task LoadAsync(ParquetRowGroupReader reader, CancellationToken cancellationToken)
    {
        RowCount = checked((int)reader.RowCount);

        if (_values.Length < RowCount)
            _values = new TStorage[RowCount];

        await reader.ReadAsync<TStorage>(Leaf(Field), _values.AsMemory(0, RowCount), null, cancellationToken).ConfigureAwait(false);
        StorageNormalization.Normalize(_values.AsSpan(0, RowCount));
    }

    public override Func<int, TValue> Getter<TValue>()
    {
        var convert = ParquetConvert<TStorage, TValue>.Func;
        return row => convert(_values[row]);
    }

    public override object? Boxed(int row) => _values[row];
}

/// <summary>A nullable value column.</summary>
internal sealed class OptionalValueColumn<TStorage>(Field field) : ParquetColumn(field)
    where TStorage : struct
{
    private TStorage?[] _values = [];

    public override async Task LoadAsync(ParquetRowGroupReader reader, CancellationToken cancellationToken)
    {
        RowCount = checked((int)reader.RowCount);

        if (_values.Length < RowCount)
            _values = new TStorage?[RowCount];

        await reader.ReadAsync<TStorage>(Leaf(Field), _values.AsMemory(0, RowCount), null, cancellationToken).ConfigureAwait(false);
        StorageNormalization.Normalize(_values.AsSpan(0, RowCount));
    }

    public override Func<int, TValue> Getter<TValue>()
    {
        var convert = ParquetConvert<TStorage, TValue>.Func;
        return row => _values[row] is { } value ? convert(value) : Missing<TValue>();
    }

    public override object? Boxed(int row) => _values[row];
}

internal sealed class StringColumn(Field field) : ParquetColumn(field)
{
    private string?[] _values = [];

    public override async Task LoadAsync(ParquetRowGroupReader reader, CancellationToken cancellationToken)
    {
        RowCount = checked((int)reader.RowCount);
        _values = new string?[RowCount];
        await reader.ReadAsync(Leaf(Field), _values.AsMemory(), null, cancellationToken).ConfigureAwait(false);
    }

    public override Func<int, TValue> Getter<TValue>()
    {
        var convert = ParquetConvert<string, TValue>.Func;
        return row => _values[row] is { } value ? convert(value) : Missing<TValue>();
    }

    public override object? Boxed(int row) => _values[row];
}

internal sealed class BinaryColumn(Field field) : ParquetColumn(field)
{
    private byte[]?[] _values = [];

    public override async Task LoadAsync(ParquetRowGroupReader reader, CancellationToken cancellationToken)
    {
        RowCount = checked((int)reader.RowCount);
        _values = new byte[]?[RowCount];
        await reader.ReadAsync(Leaf(Field), _values.AsMemory(), null, cancellationToken).ConfigureAwait(false);
    }

    public override Func<int, TValue> Getter<TValue>()
    {
        var convert = ParquetConvert<byte[], TValue>.Func;
        return row => _values[row] is { } value ? convert(value) : Missing<TValue>();
    }

    public override object? Boxed(int row) => _values[row];
}

/// <summary>A TIME column: integers in a unit, exposed as <see cref="TimeOnly" />.</summary>
internal sealed class TimeColumn(Field field, TimeUnitPrecision unit) : ParquetColumn(field)
{
    private TimeOnly?[] _values = [];

    public override async Task LoadAsync(ParquetRowGroupReader reader, CancellationToken cancellationToken)
    {
        RowCount = checked((int)reader.RowCount);
        _values = new TimeOnly?[RowCount];
        var leaf = Leaf(Field);

        if (leaf.ClrType == typeof(int))
        {
            var raw = new int?[RowCount];
            await ReadAsync(reader, leaf, raw, cancellationToken).ConfigureAwait(false);

            for (var i = 0; i < raw.Length; i++)
            {
                _values[i] = raw[i] is { } value ? StorageNormalization.Time(value, unit) : null;
            }
        }
        else
        {
            var raw = new long?[RowCount];
            await ReadAsync(reader, leaf, raw, cancellationToken).ConfigureAwait(false);

            for (var i = 0; i < raw.Length; i++)
            {
                _values[i] = raw[i] is { } value ? StorageNormalization.Time(value, unit) : null;
            }
        }
    }

    public override Func<int, TValue> Getter<TValue>()
    {
        var convert = ParquetConvert<TimeOnly, TValue>.Func;
        return row => _values[row] is { } value ? convert(value) : Missing<TValue>();
    }

    public override object? Boxed(int row) => _values[row];

    private static async Task ReadAsync<TRaw>(ParquetRowGroupReader reader, DataField leaf, TRaw?[] into, CancellationToken cancellationToken)
        where TRaw : struct
    {
        if (leaf.IsNullable)
        {
            await reader.ReadAsync<TRaw>(leaf, into.AsMemory(), null, cancellationToken).ConfigureAwait(false);
            return;
        }

        var required = new TRaw[into.Length];
        await reader.ReadAsync<TRaw>(leaf, required.AsMemory(), null, cancellationToken).ConfigureAwait(false);

        for (var i = 0; i < required.Length; i++)
        {
            into[i] = required[i];
        }
    }
}

/// <summary>Value fix-ups applied as a row group loads.</summary>
internal static class StorageNormalization
{
    public static void Normalize<T>(Span<T> values)
    {
        // Timestamps without a zone (legacy INT96, or written by other tools) are UTC, as everywhere in the connectors.
        if (typeof(T) == typeof(DateTime))
        {
            foreach (ref var value in values)
            {
                ref var date = ref System.Runtime.CompilerServices.Unsafe.As<T, DateTime>(ref value);

                if (date.Kind == DateTimeKind.Unspecified)
                    date = DateTime.SpecifyKind(date, DateTimeKind.Utc);
            }
        }
        else if (typeof(T) == typeof(DateTime?))
        {
            foreach (ref var value in values)
            {
                ref var date = ref System.Runtime.CompilerServices.Unsafe.As<T, DateTime?>(ref value);

                if (date is { Kind: DateTimeKind.Unspecified } unspecified)
                    date = DateTime.SpecifyKind(unspecified, DateTimeKind.Utc);
            }
        }
    }

    public static TimeOnly Time(long value, TimeUnitPrecision unit) =>
        new(unit switch
        {
            TimeUnitPrecision.Millis => value * TimeSpan.TicksPerMillisecond,
            TimeUnitPrecision.Micros => value * 10,
            _ => value / 100,
        });

    public static long Time(TimeOnly value) => value.Ticks / 10;
}

/// <summary>List columns, read through the raw API: Parquet.Net's level-decoding reads fail for lists (6.1.0).</summary>
internal static class ListColumns
{
    public static Func<Field, ParquetColumn> For(Type clr)
    {
        if (clr == typeof(string))
            return static f => new ListColumn<ReadOnlyMemory<char>, string?>(f, static raw => raw.ToString());

        if (clr == typeof(byte[]))
            return static f => new ListColumn<ReadOnlyMemory<byte>, byte[]?>(f, static raw => raw.ToArray());

        var method = typeof(ListColumns).GetMethod(nameof(ForValue), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(clr);
        return (Func<Field, ParquetColumn>)method.Invoke(null, null)!;
    }

    public static ParquetColumn Time(Field field, TimeUnitPrecision unit) =>
        Leaf(field).ClrType == typeof(int)
            ? new ListColumn<int, TimeOnly?>(field, raw => StorageNormalization.Time(raw, unit))
            : new ListColumn<long, TimeOnly?>(field, raw => StorageNormalization.Time(raw, unit));

    private static DataField Leaf(Field field) => (DataField)((ListField)field).Item;

    private static Func<Field, ParquetColumn> ForValue<T>()
        where T : struct =>
        typeof(T) == typeof(DateTime)
            ? static f => new ListColumn<T, T?>(f, static raw => (T?)(object)DateTime.SpecifyKind((DateTime)(object)raw, DateTimeKind.Utc))
            : static f => new ListColumn<T, T?>(f, static raw => raw);
}

/// <summary>
///     A list column. Each row becomes an array of items, or <c>null</c>; levels are decoded here: below the list's level
///     is a null list, at it an empty list, at the maximum a value, and between them a null element.
/// </summary>
internal sealed class ListColumn<TRaw, TItem>(Field field, Func<TRaw, TItem> item) : ParquetColumn(field)
    where TRaw : struct
{
    private TItem[]?[] _rows = [];

    public override async Task LoadAsync(ParquetRowGroupReader reader, CancellationToken cancellationToken)
    {
        RowCount = checked((int)reader.RowCount);
        var leaf = Leaf(Field);
        var count = checked((int)(reader.GetMetadata(leaf)?.MetaData?.NumValues ?? RowCount));
        var values = new TRaw[count];
        var definitions = new int[count];
        var repetitions = new int[count];
        await reader.ReadRawAsync<TRaw>(leaf, values.AsMemory(), definitions.AsMemory(), repetitions.AsMemory(), cancellationToken).ConfigureAwait(false);

        var maxDefinition = leaf.MaxDefinitionLevel;
        var elementNullable = leaf.IsNullable;
        var listLevel = maxDefinition - 1 - (elementNullable ? 1 : 0);
        _rows = new TItem[]?[RowCount];

        var row = -1;
        var next = 0;
        var buffer = new List<TItem>();
        var isNull = false;

        void Flush()
        {
            if (row >= 0)
                _rows[row] = isNull ? null : [.. buffer];
        }

        for (var i = 0; i < count; i++)
        {
            if (repetitions[i] == 0)
            {
                Flush();
                row++;
                buffer.Clear();
                isNull = false;
            }

            var definition = definitions[i];

            if (definition == maxDefinition)
                buffer.Add(item(values[next++]));
            else if (definition > listLevel)
                buffer.Add(default!);
            else if (definition < listLevel)
                isNull = true;
        }

        Flush();
    }

    public override Func<int, TValue> Getter<TValue>()
    {
        var element = ParquetTypeMap.ListElement(typeof(TValue))
                      ?? throw new NotSupportedException($"Column '{Name}' is a list, which cannot be read as {typeof(TValue).Name}.");

        var method = typeof(ListColumn<TRaw, TItem>).GetMethod(nameof(ListGetter), BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(typeof(TValue), element);

        return (Func<int, TValue>)method.Invoke(this, null)!;
    }

    public override object? Boxed(int row) => _rows[row];

    private Func<int, TValue> ListGetter<TValue, TElement>()
    {
        var convert = ParquetConvert<TItem, TElement>.Func;
        var asArray = typeof(TValue).IsArray;

        return row =>
        {
            if (_rows[row] is not { } items)
                return default!;

            var converted = new TElement[items.Length];

            for (var i = 0; i < items.Length; i++)
            {
                converted[i] = items[i] is null ? Missing<TElement>() : convert(items[i]);
            }

            return asArray ? (TValue)(object)converted : (TValue)(object)new List<TElement>(converted);
        };
    }
}
