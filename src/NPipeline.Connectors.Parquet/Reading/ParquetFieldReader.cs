using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Parquet.Reading;

/// <summary>
///     The row the compiled record mapper reads: a position in the current row group's typed columns. Ordinals past the
///     file's columns are partition values taken from the file's path.
/// </summary>
internal sealed class ParquetFieldReader(ParquetColumn?[] columns, IReadOnlyList<string> names, string?[] partitionValues) : IFieldReader
{
    private readonly Delegate?[] _getters = new Delegate?[columns.Length + partitionValues.Length];

    public int Row { get; set; }

    public TValue GetValue<TValue>(int ordinal) =>
        _getters[ordinal] is Func<int, TValue> getter
            ? getter(Row)
            : CreateGetter<TValue>(ordinal)(Row);

    private Func<int, TValue> CreateGetter<TValue>(int ordinal)
    {
        Func<int, TValue> getter;

        if (ordinal >= columns.Length)
        {
            var text = partitionValues[ordinal - columns.Length];
            var value = text is null ? default! : ScalarParser.Parse<TValue>(text);
            getter = _ => value;
        }
        else
        {
            getter = columns[ordinal] is { } column
                ? column.Getter<TValue>()
                : throw new NotSupportedException($"Column '{names[ordinal]}' is a nested type this connector cannot read; mark its member [IgnoreColumn].");
        }

        _getters[ordinal] = getter;
        return getter;
    }
}
