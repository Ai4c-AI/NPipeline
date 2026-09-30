using NPipeline.Connectors.Errors;
using Parquet;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet;

/// <summary>
///     A row group about to be read, for <see cref="ParquetReadOptions.RowGroupFilter" /> to skip it without reading its
///     columns. The range of a column comes from the statistics its writer recorded, when it recorded any.
/// </summary>
public sealed class ParquetRowGroupInfo
{
    private readonly Dictionary<string, DataField> _fields;
    private readonly bool _millisecondTimestamps;
    private readonly ParquetRowGroupReader _reader;

    internal ParquetRowGroupInfo(string source, int index, ParquetRowGroupReader reader, Dictionary<string, DataField> fields, bool millisecondTimestamps)
    {
        Source = source;
        Index = index;
        _reader = reader;
        _fields = fields;
        _millisecondTimestamps = millisecondTimestamps;
    }

    /// <summary>The file, without its query string.</summary>
    public string Source { get; }

    /// <summary>The row group's 0-based position in the file.</summary>
    public int Index { get; }

    /// <summary>The number of rows in the row group.</summary>
    public long RowCount => _reader.RowCount;

    /// <summary>
    ///     The smallest and largest value of a column in this row group. Returns <c>false</c> when there is no such column,
    ///     the writer recorded no statistics, or the column is unsigned (Parquet.Net reports unsigned statistics as signed).
    /// </summary>
    /// <param name="column">The column's name (case-insensitive).</param>
    /// <param name="min">The smallest value.</param>
    /// <param name="max">The largest value.</param>
    /// <typeparam name="TValue">The type to read the values as, such as <see cref="DateTime" /> or <see cref="int" />.</typeparam>
    public bool TryGetRange<TValue>(string column, out TValue min, out TValue max)
    {
        min = default!;
        max = default!;

        if (!_fields.TryGetValue(column, out var field) || field.ClrType == typeof(uint) || field.ClrType == typeof(ulong) || field.ClrType == typeof(ushort)
            || _reader.GetStatistics(field) is not { MinValue: { } low, MaxValue: { } high })
            return false;

        try
        {
            min = ParquetValue.Convert<TValue>(Decode(field, low, false));
            max = ParquetValue.Convert<TValue>(Decode(field, high, true));
            return true;
        }
        catch (FieldConversionException)
        {
            return false;
        }
    }

    /// <summary>
    ///     Statistics hold physical values: a date as days since 1970, a timestamp or time as a count of its unit.
    ///     Parquet.Net (6.1) records timestamp statistics in milliseconds whatever the column's unit, so for its files the
    ///     maximum is rounded up to the end of its millisecond, keeping the range a superset of the values.
    /// </summary>
    private object Decode(DataField field, object value, bool isMax)
    {
        switch (field)
        {
            case TimeDataField time when value is int or long:
                return Reading.StorageNormalization.Time(Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture), time.Precision);

            case DateTimeDataField { DateTimeFormat: DateTimeFormat.Date } when value is int days:
                return DateTime.UnixEpoch.AddDays(days);

            case DateTimeDataField { DateTimeFormat: DateTimeFormat.DateAndTime } when value is long milliseconds:
                return DateTime.UnixEpoch.AddTicks(milliseconds * TimeSpan.TicksPerMillisecond);

            case DateTimeDataField { DateTimeFormat: DateTimeFormat.Timestamp } timestamp when value is long count:
                var unit = _millisecondTimestamps ? DateTimeTimeUnit.Millis : timestamp.Unit;

                var at = unit switch
                {
                    DateTimeTimeUnit.Millis => DateTime.UnixEpoch.AddTicks(count * TimeSpan.TicksPerMillisecond),
                    DateTimeTimeUnit.Micros => DateTime.UnixEpoch.AddTicks(count * 10),
                    _ => DateTime.UnixEpoch.AddTicks(count / 100),
                };

                return isMax && _millisecondTimestamps && timestamp.Unit != DateTimeTimeUnit.Millis ? at.AddTicks(TimeSpan.TicksPerMillisecond - 1) : at;

            default:
                return value;
        }
    }
}
