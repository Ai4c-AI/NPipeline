using ExcelDataReader;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Excel;

/// <summary>
///     The current row, for a manual mapper passed to <see cref="ExcelSourceNode{T}" />. It is reused for every row of a
///     sheet, so read what you need inside the mapper and do not keep it.
/// </summary>
/// <remarks>
///     Cells hold numbers, text, booleans and dates. <see cref="Get{T}(string)" /> converts them strictly: a whole number
///     reads as an <c>int</c>, but <c>1.7</c> does not; text is parsed culture-invariantly; a date cell reads as a
///     <see cref="DateTime" />, <see cref="DateOnly" /> or <see cref="DateTimeOffset" />. A missing column or a value that
///     does not convert throws, naming the column. <see cref="TryGet{T}(string, out T)" /> is the lenient alternative.
/// </remarks>
public sealed class ExcelRow
{
    private readonly Dictionary<string, int> _ordinals;
    private readonly IExcelDataReader _reader;

    internal ExcelRow(IExcelDataReader reader, IReadOnlyList<string> headers)
    {
        _reader = reader;
        Headers = headers;
        _ordinals = new Dictionary<string, int>(headers.Count, StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < headers.Count; i++)
        {
            _ = _ordinals.TryAdd(headers[i], i);
        }
    }

    /// <summary>The sheet's header, or an empty list when it has none.</summary>
    public IReadOnlyList<string> Headers { get; }

    /// <summary>The name of the sheet being read.</summary>
    public string SheetName => _reader.Name;

    /// <summary>The row's 1-based position among the data rows, not counting the header or skipped rows.</summary>
    public long RecordNumber { get; internal set; }

    /// <summary>The row's 1-based row number in the sheet, as Excel shows it.</summary>
    public int RowNumber { get; internal set; }

    /// <summary>The number of cells in this row.</summary>
    public int FieldCount => _reader.FieldCount;

    /// <summary>The cell's raw value (<see cref="double" />, <see cref="string" />, <see cref="bool" /> or <see cref="DateTime" />), or <c>null</c> when it is empty or past the end of the row.</summary>
    public object? this[int index] => ExcelFieldReader.Raw(_reader, index);

    /// <summary>The named cell's raw value, or <c>null</c> when there is no such column or the cell is empty.</summary>
    public object? this[string name] => _ordinals.TryGetValue(name, out var index) ? this[index] : null;

    /// <summary>Whether the header has a column named <paramref name="name" /> (case-insensitive).</summary>
    public bool HasColumn(string name) => _ordinals.ContainsKey(name);

    /// <summary>Reads the named cell as <typeparamref name="T" />.</summary>
    /// <exception cref="FieldMappingException">There is no such column, or its value does not convert.</exception>
    public T Get<T>(string name)
    {
        if (!_ordinals.TryGetValue(name, out var index))
            throw new FieldMappingException(name, name, new KeyNotFoundException($"The sheet has no column '{name}'. Columns: {string.Join(", ", Headers)}."));

        return Read<T>(index, name);
    }

    /// <summary>Reads the cell at <paramref name="index" /> (0-based) as <typeparamref name="T" />.</summary>
    /// <exception cref="FieldMappingException">The value does not convert.</exception>
    public T Get<T>(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        return Read<T>(index, index < Headers.Count ? Headers[index] : ExcelFieldReader.ColumnLetters(index));
    }

    /// <summary>Reads the named cell as <typeparamref name="T" />, returning <c>false</c> when there is no such column or the value does not convert.</summary>
    public bool TryGet<T>(string name, out T value)
    {
        if (_ordinals.TryGetValue(name, out var index))
            return TryGet(index, out value);

        value = default!;
        return false;
    }

    /// <summary>Reads the cell at <paramref name="index" /> as <typeparamref name="T" />, returning <c>false</c> when the value does not convert.</summary>
    public bool TryGet<T>(int index, out T value)
    {
        try
        {
            value = ScalarConverter.Convert<T>(this[index]);
            return true;
        }
        catch (FieldConversionException)
        {
            value = default!;
            return false;
        }
    }

    private T Read<T>(int index, string column)
    {
        try
        {
            return ScalarConverter.Convert<T>(this[index]);
        }
        catch (FieldConversionException ex)
        {
            throw new FieldMappingException(column, column, ex);
        }
    }
}

/// <summary>The row the compiled record mapper reads: strict conversion by ordinal.</summary>
internal sealed class ExcelFieldReader(IExcelDataReader reader) : IFieldReader
{
    public TValue GetValue<TValue>(int ordinal) => ScalarConverter.Convert<TValue>(Raw(reader, ordinal));

    /// <summary>The cell's value. Dates have no time zone in a workbook; like the other connectors, they are read as UTC.</summary>
    public static object? Raw(IExcelDataReader reader, int ordinal) =>
        ordinal < 0 || ordinal >= reader.FieldCount
            ? null
            : reader.GetValue(ordinal) switch
            {
                DateTime { Kind: DateTimeKind.Unspecified } date => DateTime.SpecifyKind(date, DateTimeKind.Utc),
                var value => value,
            };

    /// <summary>The column's letters, as Excel shows them: 0 is A, 26 is AA.</summary>
    public static string ColumnLetters(int index)
    {
        Span<char> letters = stackalloc char[4];
        var position = letters.Length;

        for (var n = index + 1; n > 0; n = (n - 1) / 26)
        {
            letters[--position] = (char)('A' + ((n - 1) % 26));
        }

        return new string(letters[position..]);
    }
}
