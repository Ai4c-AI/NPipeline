using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Csv;

/// <summary>Creates CSV sources and sinks.</summary>
/// <example>
///     <code>
/// var source = CsvConnector.Source&lt;Order&gt;(StorageUri.Parse("s3://bucket/orders/*.csv"), o => o with { Delimiter = ";" });
/// var sink = CsvConnector.Sink&lt;Order&gt;(StorageUri.FromFilePath("orders.csv.gz"));
///     </code>
/// </example>
public static class CsvConnector
{
    /// <summary>A source that maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <param name="uri">A file, a directory ending in <c>/</c>, or a glob.</param>
    /// <param name="configure">Adjusts the default options, typically with a <c>with</c> expression.</param>
    public static CsvSourceNode<T> Source<T>(StorageUri uri, Func<CsvReadOptions, CsvReadOptions>? configure = null) =>
        new(ReadOptions(uri, configure));

    /// <summary>A source that builds each record with <paramref name="map" />.</summary>
    /// <param name="uri">A file, a directory ending in <c>/</c>, or a glob.</param>
    /// <param name="map">Builds a record from the current row.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static CsvSourceNode<T> Source<T>(StorageUri uri, Func<CsvRow, T> map, Func<CsvReadOptions, CsvReadOptions>? configure = null) =>
        new(ReadOptions(uri, configure), map);

    /// <summary>A sink that writes <typeparamref name="T" />'s readable members as columns.</summary>
    /// <param name="uri">The file to write.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static CsvSinkNode<T> Sink<T>(StorageUri uri, Func<CsvWriteOptions, CsvWriteOptions>? configure = null) =>
        new(WriteOptions(uri, configure));

    /// <summary>A sink that writes each record with <paramref name="write" />.</summary>
    /// <param name="uri">The file to write.</param>
    /// <param name="columns">The header.</param>
    /// <param name="write">Writes one record's fields, in <paramref name="columns" /> order.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static CsvSinkNode<T> Sink<T>(
        StorageUri uri,
        IReadOnlyList<string> columns,
        Action<CsvRowWriter, T> write,
        Func<CsvWriteOptions, CsvWriteOptions>? configure = null) =>
        new(WriteOptions(uri, configure), columns, write);

    private static CsvReadOptions ReadOptions(StorageUri uri, Func<CsvReadOptions, CsvReadOptions>? configure)
    {
        var options = new CsvReadOptions { Uri = uri };
        return configure is null ? options : configure(options);
    }

    private static CsvWriteOptions WriteOptions(StorageUri uri, Func<CsvWriteOptions, CsvWriteOptions>? configure)
    {
        var options = new CsvWriteOptions { Uri = uri };
        return configure is null ? options : configure(options);
    }
}
