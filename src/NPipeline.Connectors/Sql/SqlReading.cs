using System.Collections.Concurrent;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using System.Text;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Sql;

/// <summary>
///     Reads one column's values as <typeparamref name="TValue" />: straight from the provider when the column already
///     holds that type, and through <see cref="ScalarConverter" /> (culture-invariant, strict) when it does not.
/// </summary>
internal static class SqlValueReader<TValue>
{
    private static readonly ConcurrentDictionary<Type, Func<DbDataReader, int, TValue>> Readers = new();
    private static readonly Type Underlying = Nullable.GetUnderlyingType(typeof(TValue)) ?? typeof(TValue);
    private static readonly bool AcceptsNull = default(TValue) is null;

    public static Func<DbDataReader, int, TValue> For(Type columnType) => Readers.GetOrAdd(columnType, Create);

    private static Func<DbDataReader, int, TValue> Create(Type columnType)
    {
        if (columnType == typeof(TValue))
            return static (reader, ordinal) => reader.IsDBNull(ordinal) ? Missing() : reader.GetFieldValue<TValue>(ordinal);

        if (columnType == Underlying)
        {
            // A nullable member over a column of its underlying type: read the underlying type and wrap it.
            var method = typeof(SqlValueReader<TValue>).GetMethod(nameof(Wrapped), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(Underlying);
            return method.CreateDelegate<Func<DbDataReader, int, TValue>>();
        }

        return static (reader, ordinal) => reader.IsDBNull(ordinal) ? Missing() : ScalarConverter.Convert<TValue>(reader.GetValue(ordinal));
    }

    private static TValue Wrapped<TUnderlying>(DbDataReader reader, int ordinal)
        where TUnderlying : struct =>
        reader.IsDBNull(ordinal) ? default! : (TValue)(object)(TUnderlying?)reader.GetFieldValue<TUnderlying>(ordinal);

    private static TValue Missing() =>
        AcceptsNull ? default! : throw new FieldConversionException(typeof(TValue), null, "the value is NULL");
}

/// <summary>The row the compiled mapper reads: each value at its bound ordinal, typed, with a reader chosen once per column.</summary>
internal sealed class SqlFieldReader(DbDataReader reader) : IFieldReader
{
    private readonly Delegate?[] _readers = new Delegate?[reader.FieldCount];

    public TValue GetValue<TValue>(int ordinal)
    {
        if (_readers[ordinal] is not Func<DbDataReader, int, TValue> read)
            _readers[ordinal] = read = SqlValueReader<TValue>.For(reader.GetFieldType(ordinal));

        return read(reader, ordinal);
    }
}

/// <summary>
///     The current row of a SQL source, for a manual mapper. The same instance is reused for every row, so read what you
///     need inside the mapper.
/// </summary>
public sealed class SqlRow
{
    private readonly DbDataReader _reader;
    private readonly Dictionary<string, int> _ordinals;

    internal SqlRow(DbDataReader reader, IReadOnlyList<string> columnNames)
    {
        _reader = reader;
        ColumnNames = columnNames;
        _ordinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < columnNames.Count; i++)
        {
            _ = _ordinals.TryAdd(columnNames[i], i);
        }
    }

    /// <summary>The result's column names, in order.</summary>
    public IReadOnlyList<string> ColumnNames { get; }

    /// <summary>The number of columns.</summary>
    public int FieldCount => ColumnNames.Count;

    /// <summary>The row's 1-based position in the result.</summary>
    public long RecordNumber { get; internal set; }

    /// <summary>The named column's value as the provider returns it, or <c>null</c> for SQL <c>NULL</c> or a missing column.</summary>
    public object? this[string columnName] => _ordinals.TryGetValue(columnName, out var ordinal) ? this[ordinal] : null;

    /// <summary>The value at <paramref name="ordinal" /> as the provider returns it, or <c>null</c> for SQL <c>NULL</c>.</summary>
    public object? this[int ordinal] => _reader.IsDBNull(ordinal) ? null : _reader.GetValue(ordinal);

    /// <summary>Whether the result has the named column (case-insensitive).</summary>
    public bool HasColumn(string columnName) => _ordinals.ContainsKey(columnName);

    /// <summary>The named column's position.</summary>
    /// <exception cref="FieldMappingException">The result has no such column.</exception>
    public int GetOrdinal(string columnName) =>
        _ordinals.TryGetValue(columnName, out var ordinal)
            ? ordinal
            : throw new FieldMappingException(columnName, columnName,
                new KeyNotFoundException($"The result has no column '{columnName}'. Columns: {string.Join(", ", ColumnNames)}."));

    /// <summary>Whether the named column is SQL <c>NULL</c>, or missing.</summary>
    public bool IsNull(string columnName) => !_ordinals.TryGetValue(columnName, out var ordinal) || _reader.IsDBNull(ordinal);

    /// <summary>Reads the named column as <typeparamref name="T" />.</summary>
    /// <exception cref="FieldMappingException">There is no such column, or its value does not convert.</exception>
    public T Get<T>(string columnName) => Get<T>(GetOrdinal(columnName));

    /// <summary>Reads the column at <paramref name="ordinal" /> as <typeparamref name="T" />.</summary>
    /// <exception cref="FieldMappingException">The value does not convert.</exception>
    public T Get<T>(int ordinal)
    {
        try
        {
            return SqlValueReader<T>.For(_reader.GetFieldType(ordinal))(_reader, ordinal);
        }
        catch (Exception ex) when (ex is FieldConversionException or InvalidCastException or FormatException or OverflowException)
        {
            var name = ColumnNames[ordinal];
            throw new FieldMappingException(name, name, ex);
        }
    }

    /// <summary>Reads the named column as <typeparamref name="T" />, returning <c>false</c> when it is missing or does not convert.</summary>
    public bool TryGet<T>(string columnName, out T value)
    {
        if (_ordinals.TryGetValue(columnName, out var ordinal))
        {
            try
            {
                value = SqlValueReader<T>.For(_reader.GetFieldType(ordinal))(_reader, ordinal);
                return true;
            }
            catch (Exception ex) when (ex is FieldConversionException or InvalidCastException or FormatException or OverflowException)
            {
            }
        }

        value = default!;
        return false;
    }

    /// <summary>The named column as <typeparamref name="T" />, or <paramref name="defaultValue" /> when it is missing, <c>NULL</c> or does not convert.</summary>
    public T GetOrDefault<T>(string columnName, T defaultValue = default!) =>
        !IsNull(columnName) && TryGet<T>(columnName, out var value) ? value : defaultValue;

    /// <summary>The row's values as <c>column=value</c> pairs, for a row error's excerpt.</summary>
    internal string Describe()
    {
        var text = new StringBuilder();

        for (var i = 0; i < ColumnNames.Count; i++)
        {
            if (i > 0)
                text.Append(", ");

            text.Append(ColumnNames[i]).Append('=');

            if (_reader.IsDBNull(i))
                text.Append("NULL");
            else
                text.Append(Convert.ToString(_reader.GetValue(i), CultureInfo.InvariantCulture));
        }

        return text.ToString();
    }
}
