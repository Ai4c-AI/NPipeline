using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Excel;

/// <summary>Creates Excel sources and sinks.</summary>
/// <example>
///     <code>
/// var source = ExcelConnector.Source&lt;Order&gt;(StorageUri.Parse("s3://bucket/orders.xlsx"), o => o with { SheetName = "Orders" });
/// var sink = ExcelConnector.Sink&lt;Order&gt;(StorageUri.FromFilePath("report.xlsx"), o => o with { FreezeHeader = true, AutoFilter = true });
///     </code>
/// </example>
public static class ExcelConnector
{
    /// <summary>A source that maps columns to <typeparamref name="T" />'s members by header.</summary>
    /// <param name="uri">A workbook, a directory ending in <c>/</c>, or a glob.</param>
    /// <param name="configure">Adjusts the default options, typically with a <c>with</c> expression.</param>
    public static ExcelSourceNode<T> Source<T>(StorageUri uri, Func<ExcelReadOptions, ExcelReadOptions>? configure = null) =>
        new(ReadOptions(uri, configure));

    /// <summary>A source that builds each record with <paramref name="map" />.</summary>
    /// <param name="uri">A workbook, a directory ending in <c>/</c>, or a glob.</param>
    /// <param name="map">Builds a record from the current row.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static ExcelSourceNode<T> Source<T>(StorageUri uri, Func<ExcelRow, T> map, Func<ExcelReadOptions, ExcelReadOptions>? configure = null) =>
        new(ReadOptions(uri, configure), map);

    /// <summary>A sink that writes <typeparamref name="T" />'s readable members as columns.</summary>
    /// <param name="uri">The workbook to write.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static ExcelSinkNode<T> Sink<T>(StorageUri uri, Func<ExcelWriteOptions, ExcelWriteOptions>? configure = null) =>
        new(WriteOptions(uri, configure));

    /// <summary>A sink that writes each record with <paramref name="write" />.</summary>
    /// <param name="uri">The workbook to write.</param>
    /// <param name="columns">The header.</param>
    /// <param name="write">Writes one record's cells, in <paramref name="columns" /> order.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static ExcelSinkNode<T> Sink<T>(
        StorageUri uri,
        IReadOnlyList<string> columns,
        Action<ExcelRowWriter, T> write,
        Func<ExcelWriteOptions, ExcelWriteOptions>? configure = null) =>
        new(WriteOptions(uri, configure), columns, write);

    private static ExcelReadOptions ReadOptions(StorageUri uri, Func<ExcelReadOptions, ExcelReadOptions>? configure)
    {
        var options = new ExcelReadOptions { Uri = uri };
        return configure is null ? options : configure(options);
    }

    private static ExcelWriteOptions WriteOptions(StorageUri uri, Func<ExcelWriteOptions, ExcelWriteOptions>? configure)
    {
        var options = new ExcelWriteOptions { Uri = uri };
        return configure is null ? options : configure(options);
    }
}
