using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Nodes;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.DuckDB;

/// <summary>Creates DuckDB sources and sinks, over a database or straight over files.</summary>
/// <example>
///     <code>
/// var orders = DuckDBConnector.FromFile&lt;Order&gt;("s3://bucket/orders/*.parquet");
/// var totals = DuckDBConnector.Source&lt;Total&gt;(DuckDBDatabase.File("sales.duckdb"), "SELECT region, sum(amount) AS total FROM sales GROUP BY region");
/// var sink = DuckDBConnector.ToFile&lt;Order&gt;("out/orders.parquet");
///     </code>
/// </example>
public static class DuckDBConnector
{
    /// <summary>A source that runs <paramref name="query" /> against <paramref name="database" /> and maps columns to members by name.</summary>
    /// <param name="database">The database; <see cref="DuckDBDatabase.InMemory" /> for queries over files.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options, typically with a <c>with</c> expression.</param>
    public static DuckDBSourceNode<T> Source<T>(DuckDBDatabase database, string query, Func<DuckDBReadOptions, DuckDBReadOptions>? configure = null) =>
        new(Read(new DuckDBReadOptions { Database = database, Query = query }, configure));

    /// <summary>A source that runs <paramref name="query" /> and builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="database">The database.</param>
    /// <param name="query">The query.</param>
    /// <param name="map">Builds a record from a row.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static DuckDBSourceNode<T> Source<T>(DuckDBDatabase database, string query, Func<SqlRow, T> map,
        Func<DuckDBReadOptions, DuckDBReadOptions>? configure = null) =>
        new(Read(new DuckDBReadOptions { Database = database, Query = query }, configure), map ?? throw new ArgumentNullException(nameof(map)));

    /// <summary>
    ///     A source over a Parquet, CSV or JSON file, or a glob of them, read by DuckDB in an in-memory database. The format
    ///     comes from the extension (<c>.parquet</c>, <c>.csv</c>, <c>.tsv</c>, <c>.json</c>, <c>.ndjson</c>, <c>.jsonl</c>).
    /// </summary>
    /// <param name="path">The file, glob or URL (URLs need the <c>httpfs</c> extension in the options' database).</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static DuckDBSourceNode<T> FromFile<T>(string path, Func<DuckDBReadOptions, DuckDBReadOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var extension = Path.GetExtension(path.Replace("*", "", StringComparison.Ordinal)).ToLowerInvariant();

        var function = extension switch
        {
            ".parquet" => "read_parquet",
            ".csv" or ".tsv" => "read_csv",
            ".json" or ".ndjson" or ".jsonl" => "read_json",
            _ => throw new NotSupportedException($"DuckDB reads .parquet, .csv, .tsv, .json, .ndjson and .jsonl files, not '{extension}'."),
        };

        return Source<T>(DuckDBDatabase.InMemory, $"SELECT * FROM {function}('{path.Replace("'", "''", StringComparison.Ordinal)}')", configure);
    }

    /// <summary>A sink that writes <typeparamref name="T" />'s readable members to <paramref name="table" /> in <paramref name="database" />.</summary>
    /// <param name="database">The database.</param>
    /// <param name="table">The table, created when missing unless <see cref="DuckDBWriteOptions.AutoCreateTable" /> is off.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static DuckDBSinkNode<T> Sink<T>(DuckDBDatabase database, string table, Func<DuckDBWriteOptions, DuckDBWriteOptions>? configure = null) =>
        new(Write(new DuckDBWriteOptions { Database = database, Table = table }, configure));

    /// <summary>
    ///     A sink that writes records to a Parquet, CSV or JSON file through a staging table in an in-memory database. The
    ///     format comes from the extension, or from <see cref="DuckDBWriteOptions.Export" />.
    /// </summary>
    /// <param name="path">The file to write.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static DuckDBSinkNode<T> ToFile<T>(string path, Func<DuckDBWriteOptions, DuckDBWriteOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        return Sink<T>(DuckDBDatabase.InMemory, $"npipeline_export_{Guid.NewGuid():N}", o => (configure ?? (x => x))(o with { ExportTo = path }));
    }

    private static DuckDBReadOptions Read(DuckDBReadOptions options, Func<DuckDBReadOptions, DuckDBReadOptions>? configure) =>
        configure is null ? options : configure(options);

    private static DuckDBWriteOptions Write(DuckDBWriteOptions options, Func<DuckDBWriteOptions, DuckDBWriteOptions>? configure) =>
        configure is null ? options : configure(options);
}
