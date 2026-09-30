using System.Collections;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Parquet.Reading;
using NPipeline.Connectors.Parquet.Schema;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet;

/// <summary>
///     One row of a Parquet file, for a manual mapper, a <see cref="ParquetReadOptions.RowFilter" />, or code that works
///     with rows whose type is not known in advance. It is a self-contained snapshot: it stays valid after the source moves
///     on, so rows can be collected.
/// </summary>
/// <remarks>
///     Values are held in the file's storage types: numbers, <see cref="string" />, <c>byte[]</c>, <see cref="Guid" />,
///     <see cref="DateTime" /> (UTC), <see cref="TimeOnly" />, and arrays for lists. <see cref="Get{T}(string)" /> converts
///     them strictly, as the attribute mapping does: a missing column or a value that does not convert throws, naming the
///     column. The attribute mapping never builds rows, so it is much faster.
/// </remarks>
public sealed class ParquetRow
{
    private readonly ParquetRowLayout _layout;
    private readonly object?[] _values;

    internal ParquetRow(ParquetRowLayout layout, object?[] values, long recordNumber)
    {
        _layout = layout;
        _values = values;
        RecordNumber = recordNumber;
    }

    /// <summary>The file's schema.</summary>
    public ParquetSchema Schema => _layout.Schema;

    /// <summary>The number of columns the row holds.</summary>
    public int ColumnCount => _values.Length;

    /// <summary>The columns the row holds, in file order.</summary>
    public IReadOnlyList<string> ColumnNames => _layout.Names;

    /// <summary>The row's 1-based position in its file.</summary>
    public long RecordNumber { get; }

    /// <summary>The raw value of the named column (case-insensitive), or <c>null</c> when it is null or the row has no such column.</summary>
    public object? this[string columnName] => _layout.TryGetOrdinal(columnName, out var ordinal) ? _values[ordinal] : null;

    /// <summary>The raw value of the column at <paramref name="index" />.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="index" /> is not a column.</exception>
    public object? this[int index] => index >= 0 && index < _values.Length
        ? _values[index]
        : throw new ArgumentOutOfRangeException(nameof(index), index, $"The row has {_values.Length} columns.");

    /// <summary>Whether the row has a column named <paramref name="columnName" /> (case-insensitive).</summary>
    public bool HasColumn(string columnName) => _layout.TryGetOrdinal(columnName, out _);

    /// <summary>Whether the named column is null, or missing.</summary>
    public bool IsNull(string columnName) => this[columnName] is null;

    /// <summary>Reads the named column as <typeparamref name="T" />.</summary>
    /// <exception cref="FieldMappingException">There is no such column, or its value does not convert.</exception>
    public T Get<T>(string columnName)
    {
        if (!_layout.TryGetOrdinal(columnName, out var ordinal))
            throw new FieldMappingException(columnName, columnName, new KeyNotFoundException($"The row has no column '{columnName}'. Columns: {string.Join(", ", ColumnNames)}."));

        return Read<T>(ordinal);
    }

    /// <summary>Reads the column at <paramref name="index" /> as <typeparamref name="T" />.</summary>
    /// <exception cref="FieldMappingException">The value does not convert.</exception>
    public T Get<T>(int index)
    {
        _ = this[index];
        return Read<T>(index);
    }

    /// <summary>Reads the named column as <typeparamref name="T" />, returning <c>false</c> when it is missing or does not convert.</summary>
    public bool TryGet<T>(string columnName, out T value)
    {
        try
        {
            if (_layout.TryGetOrdinal(columnName, out var ordinal))
            {
                value = ParquetValue.Convert<T>(_values[ordinal]);
                return true;
            }
        }
        catch (FieldConversionException)
        {
        }

        value = default!;
        return false;
    }

    /// <summary>Reads the named column as <typeparamref name="T" />, or returns <paramref name="defaultValue" /> when it is missing, null or does not convert.</summary>
    public T GetOrDefault<T>(string columnName, T defaultValue = default!) =>
        this[columnName] is not null && TryGet<T>(columnName, out var value) ? value : defaultValue;

    private T Read<T>(int ordinal)
    {
        try
        {
            return ParquetValue.Convert<T>(_values[ordinal]);
        }
        catch (FieldConversionException ex)
        {
            throw new FieldMappingException(_layout.Names[ordinal], _layout.Names[ordinal], ex);
        }
    }
}

/// <summary>A file's columns, shared by its rows.</summary>
internal sealed class ParquetRowLayout
{
    private readonly Dictionary<string, int> _ordinals;

    public ParquetRowLayout(ParquetSchema schema, IReadOnlyList<string> names)
    {
        Schema = schema;
        Names = names;
        _ordinals = new Dictionary<string, int>(names.Count, StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < names.Count; i++)
        {
            _ = _ordinals.TryAdd(names[i], i);
        }
    }

    public ParquetSchema Schema { get; }

    public IReadOnlyList<string> Names { get; }

    public bool TryGetOrdinal(string name, out int ordinal) => _ordinals.TryGetValue(name, out ordinal);
}

/// <summary>Converts a raw storage value, boxed, as the typed column readers would.</summary>
internal static class ParquetValue
{
    public static T Convert<T>(object? value) =>
        value switch
        {
            null => default(T) is null ? default! : throw new FieldConversionException(typeof(T), null, "the value is missing"),
            T typed when typeof(T) != typeof(DateTime) => typed,
            DateTime date => ParquetConvert<DateTime, T>.Func(date.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(date, DateTimeKind.Utc) : date),
            string text => ParquetConvert<string, T>.Func(text),
            long number => ParquetConvert<long, T>.Func(number),
            TimeOnly time => ParquetConvert<TimeOnly, T>.Func(time),
            Array items when typeof(T) != typeof(byte[]) => ConvertList<T>(items),
            _ => NPipeline.Connectors.Mapping.ScalarConverter.Convert<T>(value),
        };

    private static T ConvertList<T>(Array items)
    {
        var element = ParquetTypeMap.ListElement(typeof(T)) ?? throw new FieldConversionException(typeof(T), null, "the value is a list");
        var converted = Array.CreateInstance(element, items.Length);
        var convert = typeof(ParquetValue).GetMethod(nameof(Convert))!.MakeGenericMethod(element);

        for (var i = 0; i < items.Length; i++)
        {
            converted.SetValue(convert.Invoke(null, [items.GetValue(i)]), i);
        }

        if (typeof(T).IsArray)
            return (T)(object)converted;

        var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(element), items.Length)!;

        foreach (var item in converted)
        {
            _ = list.Add(item);
        }

        return (T)list;
    }
}
