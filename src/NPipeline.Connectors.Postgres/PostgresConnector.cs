using NPipeline.Connectors.Postgres.Configuration;
using NPipeline.Connectors.Postgres.Connection;
using NPipeline.Connectors.Postgres.Nodes;
using NPipeline.Connectors.Sql;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Postgres;

/// <summary>Creates PostgreSQL sources and sinks.</summary>
/// <example>
///     <code>
/// var source = PostgresConnector.Source&lt;Order&gt;(connectionString, "SELECT * FROM orders ORDER BY id");
/// var sink = PostgresConnector.Sink&lt;Order&gt;(connectionString, "orders", o => o with { WriteStrategy = PostgresWriteStrategy.Copy });
///     </code>
/// </example>
public static class PostgresConnector
{
    /// <summary>A source that runs <paramref name="query" /> and maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options, typically with a <c>with</c> expression.</param>
    public static PostgresSourceNode<T> Source<T>(string connectionString, string query, Func<PostgresReadOptions, PostgresReadOptions>? configure = null) =>
        new(Read(new PostgresReadOptions { ConnectionString = connectionString, Query = query }, configure));

    /// <summary>A source that runs <paramref name="query" /> and builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="query">The query.</param>
    /// <param name="map">Builds a record from a row.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static PostgresSourceNode<T> Source<T>(string connectionString, string query, Func<SqlRow, T> map,
        Func<PostgresReadOptions, PostgresReadOptions>? configure = null) =>
        new(Read(new PostgresReadOptions { ConnectionString = connectionString, Query = query }, configure), map ?? throw new ArgumentNullException(nameof(map)));

    /// <summary>A source on a database named by a storage URI (<c>postgres://…</c>).</summary>
    /// <param name="uri">The database URI.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static PostgresSourceNode<T> Source<T>(StorageUri uri, string query, Func<PostgresReadOptions, PostgresReadOptions>? configure = null) =>
        new(Read(new PostgresReadOptions { Uri = uri, Query = query }, configure));

    /// <summary>A source on a connection from <paramref name="pool" />; set <see cref="PostgresReadOptions.ConnectionName" /> for a named one.</summary>
    /// <param name="pool">The connection pool.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static PostgresSourceNode<T> Source<T>(IPostgresConnectionPool pool, string query, Func<PostgresReadOptions, PostgresReadOptions>? configure = null) =>
        new(Read(new PostgresReadOptions { ConnectionPool = pool, Query = query }, configure));

    /// <summary>A sink that writes <typeparamref name="T" />'s readable members to <paramref name="table" />.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static PostgresSinkNode<T> Sink<T>(string connectionString, string table, Func<PostgresWriteOptions, PostgresWriteOptions>? configure = null) =>
        new(Write(new PostgresWriteOptions { ConnectionString = connectionString, Table = table }, configure));

    /// <summary>A sink on a database named by a storage URI (<c>postgres://…</c>).</summary>
    /// <param name="uri">The database URI.</param>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static PostgresSinkNode<T> Sink<T>(StorageUri uri, string table, Func<PostgresWriteOptions, PostgresWriteOptions>? configure = null) =>
        new(Write(new PostgresWriteOptions { Uri = uri, Table = table }, configure));

    /// <summary>A sink on a connection from <paramref name="pool" />; set <see cref="PostgresWriteOptions.ConnectionName" /> for a named one.</summary>
    /// <param name="pool">The connection pool.</param>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static PostgresSinkNode<T> Sink<T>(IPostgresConnectionPool pool, string table, Func<PostgresWriteOptions, PostgresWriteOptions>? configure = null) =>
        new(Write(new PostgresWriteOptions { ConnectionPool = pool, Table = table }, configure));

    private static PostgresReadOptions Read(PostgresReadOptions options, Func<PostgresReadOptions, PostgresReadOptions>? configure) =>
        configure is null ? options : configure(options);

    private static PostgresWriteOptions Write(PostgresWriteOptions options, Func<PostgresWriteOptions, PostgresWriteOptions>? configure) =>
        configure is null ? options : configure(options);
}
