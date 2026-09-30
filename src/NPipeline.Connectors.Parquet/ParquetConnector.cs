using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Parquet;

/// <summary>Creates Parquet sources and sinks.</summary>
/// <example>
///     <code>
/// var source = ParquetConnector.Source&lt;Order&gt;(StorageUri.Parse("s3://bucket/orders/"), o => o with { FileReadParallelism = 4 });
/// var sink = ParquetConnector.Sink&lt;Order&gt;(StorageUri.FromFilePath("orders.parquet"), o => o with { Codec = CompressionMethod.Zstd });
///     </code>
/// </example>
public static class ParquetConnector
{
    /// <summary>A source that maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <param name="uri">A file, a directory ending in <c>/</c>, or a glob.</param>
    /// <param name="configure">Adjusts the default options, typically with a <c>with</c> expression.</param>
    public static ParquetSourceNode<T> Source<T>(StorageUri uri, Func<ParquetReadOptions, ParquetReadOptions>? configure = null) =>
        new(ReadOptions(uri, configure));

    /// <summary>A source that builds each record from a <see cref="ParquetRow" /> with <paramref name="map" />.</summary>
    /// <param name="uri">A file, a directory ending in <c>/</c>, or a glob.</param>
    /// <param name="map">Builds a record from a row.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static ParquetSourceNode<T> Source<T>(StorageUri uri, Func<ParquetRow, T> map, Func<ParquetReadOptions, ParquetReadOptions>? configure = null) =>
        new(ReadOptions(uri, configure), map);

    /// <summary>A sink that writes <typeparamref name="T" />'s readable members as columns.</summary>
    /// <param name="uri">The file to write.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static ParquetSinkNode<T> Sink<T>(StorageUri uri, Func<ParquetWriteOptions, ParquetWriteOptions>? configure = null) =>
        new(WriteOptions(uri, configure));

    private static ParquetReadOptions ReadOptions(StorageUri uri, Func<ParquetReadOptions, ParquetReadOptions>? configure)
    {
        var options = new ParquetReadOptions { Uri = uri };
        return configure is null ? options : configure(options);
    }

    private static ParquetWriteOptions WriteOptions(StorageUri uri, Func<ParquetWriteOptions, ParquetWriteOptions>? configure)
    {
        var options = new ParquetWriteOptions { Uri = uri };
        return configure is null ? options : configure(options);
    }
}
