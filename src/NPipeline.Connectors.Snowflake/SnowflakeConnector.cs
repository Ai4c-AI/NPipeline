using NPipeline.Connectors.Sql;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Connection;
using NPipeline.Connectors.Snowflake.Nodes;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Snowflake;

/// <summary>Creates Snowflake sources and sinks.</summary>
/// <example>
///     <code>
/// var source = SnowflakeConnector.Source&lt;Order&gt;(connectionString, "SELECT * FROM ORDERS ORDER BY Id");
/// var sink = SnowflakeConnector.Sink&lt;Order&gt;(connectionString, "ORDERS", o => o with { WriteStrategy = SnowflakeWriteStrategy.StagedCopy });
///     </code>
/// </example>
public static class SnowflakeConnector
{
    /// <summary>A source that runs <paramref name="query" /> and maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options, typically with a <c>with</c> expression.</param>
    public static SnowflakeSourceNode<T> Source<T>(string connectionString, string query, Func<SnowflakeReadOptions, SnowflakeReadOptions>? configure = null) =>
        new(Read(new SnowflakeReadOptions { ConnectionString = connectionString, Query = query }, configure));

    /// <summary>A source that runs <paramref name="query" /> and builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="query">The query.</param>
    /// <param name="map">Builds a record from a row.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static SnowflakeSourceNode<T> Source<T>(string connectionString, string query, Func<SqlRow, T> map,
        Func<SnowflakeReadOptions, SnowflakeReadOptions>? configure = null) =>
        new(Read(new SnowflakeReadOptions { ConnectionString = connectionString, Query = query }, configure), map ?? throw new ArgumentNullException(nameof(map)));

    /// <summary>A source on a database named by a storage URI (<c>snowflake://…</c>).</summary>
    /// <param name="uri">The database URI.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static SnowflakeSourceNode<T> Source<T>(StorageUri uri, string query, Func<SnowflakeReadOptions, SnowflakeReadOptions>? configure = null) =>
        new(Read(new SnowflakeReadOptions { Uri = uri, Query = query }, configure));

    /// <summary>A source on a connection from <paramref name="pool" />; set <see cref="SnowflakeReadOptions.ConnectionName" /> for a named one.</summary>
    /// <param name="pool">The connection pool.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static SnowflakeSourceNode<T> Source<T>(ISnowflakeConnectionPool pool, string query, Func<SnowflakeReadOptions, SnowflakeReadOptions>? configure = null) =>
        new(Read(new SnowflakeReadOptions { ConnectionPool = pool, Query = query }, configure));

    /// <summary>A sink that writes <typeparamref name="T" />'s readable members to <paramref name="table" />.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static SnowflakeSinkNode<T> Sink<T>(string connectionString, string table, Func<SnowflakeWriteOptions, SnowflakeWriteOptions>? configure = null) =>
        new(Write(new SnowflakeWriteOptions { ConnectionString = connectionString, Table = table }, configure));

    /// <summary>A sink on a database named by a storage URI (<c>snowflake://…</c>).</summary>
    /// <param name="uri">The database URI.</param>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static SnowflakeSinkNode<T> Sink<T>(StorageUri uri, string table, Func<SnowflakeWriteOptions, SnowflakeWriteOptions>? configure = null) =>
        new(Write(new SnowflakeWriteOptions { Uri = uri, Table = table }, configure));

    /// <summary>A sink on a connection from <paramref name="pool" />; set <see cref="SnowflakeWriteOptions.ConnectionName" /> for a named one.</summary>
    /// <param name="pool">The connection pool.</param>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static SnowflakeSinkNode<T> Sink<T>(ISnowflakeConnectionPool pool, string table, Func<SnowflakeWriteOptions, SnowflakeWriteOptions>? configure = null) =>
        new(Write(new SnowflakeWriteOptions { ConnectionPool = pool, Table = table }, configure));

    private static SnowflakeReadOptions Read(SnowflakeReadOptions options, Func<SnowflakeReadOptions, SnowflakeReadOptions>? configure) =>
        configure is null ? options : configure(options);

    private static SnowflakeWriteOptions Write(SnowflakeWriteOptions options, Func<SnowflakeWriteOptions, SnowflakeWriteOptions>? configure) =>
        configure is null ? options : configure(options);
}
