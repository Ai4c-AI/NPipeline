using CsvHelper;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Csv;

/// <summary>
///     The current row, for a manual mapper passed to <see cref="CsvSourceNode{T}" />. It is reused for every row of a
///     file, so read what you need inside the mapper and do not keep it.
/// </summary>
/// <remarks>
///     <see cref="Get{T}(string)" /> and <see cref="Get{T}(int)" /> are strict: a missing column or a value that does not
///     convert throws, naming the column, and the source's <c>RowErrorHandler</c> decides what happens to the row.
///     <see cref="TryGet{T}(string, out T)" /> is the lenient alternative.
/// </remarks>
public sealed class CsvRow
{
    private readonly IFormatProvider _culture;
    private readonly Dictionary<string, int> _ordinals;
    private readonly CsvParser _parser;

    internal CsvRow(CsvParser parser, IReadOnlyList<string> headers, IFormatProvider culture)
    {
        _parser = parser;
        _culture = culture;
        Headers = headers;
        _ordinals = new Dictionary<string, int>(headers.Count, StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < headers.Count; i++)
        {
            // The first of duplicate headers wins, as in the binder.
            _ = _ordinals.TryAdd(headers[i], i);
        }
    }

    /// <summary>The file's header, or an empty list when it has none.</summary>
    public IReadOnlyList<string> Headers { get; }

    /// <summary>The row's 1-based position among the data rows, not counting the header.</summary>
    public long RecordNumber { get; internal set; }

    /// <summary>The number of fields in this row.</summary>
    public int FieldCount => _parser.Count;

    /// <summary>The row as it appears in the file.</summary>
    public string RawRecord => _parser.RawRecord;

    /// <summary>The raw text of the field at <paramref name="index" />, or <c>null</c> when the row is shorter.</summary>
    public string? this[int index] => index >= 0 && index < _parser.Count ? _parser[index] : null;

    /// <summary>The raw text of the named field, or <c>null</c> when there is no such column or the row is shorter.</summary>
    public string? this[string name] => _ordinals.TryGetValue(name, out var index) ? this[index] : null;

    /// <summary>Whether the header has a column named <paramref name="name" /> (case-insensitive).</summary>
    public bool HasColumn(string name) => _ordinals.ContainsKey(name);

    /// <summary>Reads the named field as <typeparamref name="T" />.</summary>
    /// <exception cref="FieldMappingException">There is no such column, or its value does not convert.</exception>
    public T Get<T>(string name)
    {
        if (!_ordinals.TryGetValue(name, out var index))
            throw new FieldMappingException(name, name, new KeyNotFoundException($"The file has no column '{name}'. Columns: {string.Join(", ", Headers)}."));

        return Read<T>(index, name);
    }

    /// <summary>Reads the field at <paramref name="index" /> as <typeparamref name="T" />. A field past the end of the row reads as empty text.</summary>
    /// <exception cref="FieldMappingException">The value does not convert.</exception>
    public T Get<T>(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        return Read<T>(index, index < Headers.Count ? Headers[index] : $"#{index}");
    }

    /// <summary>Reads the named field as <typeparamref name="T" />, returning <c>false</c> when there is no such column or the value does not convert.</summary>
    public bool TryGet<T>(string name, out T value)
    {
        if (_ordinals.TryGetValue(name, out var index))
            return TryGet(index, out value);

        value = default!;
        return false;
    }

    /// <summary>Reads the field at <paramref name="index" /> as <typeparamref name="T" />, returning <c>false</c> when the row is shorter or the value does not convert.</summary>
    public bool TryGet<T>(int index, out T value)
    {
        if (index >= 0 && index < _parser.Count && ScalarParser.TryParse<T>(_parser[index], _culture, out var parsed))
        {
            value = parsed;
            return true;
        }

        value = default!;
        return false;
    }

    private T Read<T>(int index, string column)
    {
        try
        {
            return ScalarParser.Parse<T>(index < _parser.Count ? _parser[index] : string.Empty, _culture);
        }
        catch (FieldConversionException ex)
        {
            throw new FieldMappingException(column, column, ex);
        }
    }
}

/// <summary>The row the compiled record mapper reads: strict conversion by ordinal.</summary>
internal sealed class CsvFieldReader(CsvParser parser, IFormatProvider culture) : IFieldReader
{
    // A field past the end of a short row reads as empty text: null for nullable members, an error for the rest.
    public TValue GetValue<TValue>(int ordinal) =>
        ScalarParser.Parse<TValue>(ordinal < parser.Count ? parser[ordinal] : string.Empty, culture);
}
