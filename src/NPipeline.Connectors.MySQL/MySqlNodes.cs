using NPipeline.Connectors.Sql;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Connection;
using NPipeline.Connectors.MySql.Nodes;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MySql;

/// <summary>
///     Creates MySQL sources and sinks. (Not <c>MySqlConnector</c>, the name of the ADO.NET driver's namespace, which the
///     class would clash with wherever the driver is imported.)
/// </summary>
/// <example>
///     <code>
/// var source = MySqlNodes.Source&lt;Order&gt;(connectionString, "SELECT * FROM orders ORDER BY Id");
/// var sink = MySqlNodes.Sink&lt;Order&gt;(connectionString, "orders", o => o with { WriteStrategy = MySqlWriteStrategy.BulkLoad });
///     </code>
/// </example>
public static class MySqlNodes
{
    /// <summary>A source that runs <paramref name="query" /> and maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options, typically with a <c>with</c> expression.</param>
    public static MySqlSourceNode<T> Source<T>(string connectionString, string query, Func<MySqlReadOptions, MySqlReadOptions>? configure = null) =>
        new(Read(new MySqlReadOptions { ConnectionString = connectionString, Query = query }, configure));

    /// <summary>A source that runs <paramref name="query" /> and builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="query">The query.</param>
    /// <param name="map">Builds a record from a row.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static MySqlSourceNode<T> Source<T>(string connectionString, string query, Func<SqlRow, T> map,
        Func<MySqlReadOptions, MySqlReadOptions>? configure = null) =>
        new(Read(new MySqlReadOptions { ConnectionString = connectionString, Query = query }, configure), map ?? throw new ArgumentNullException(nameof(map)));

    /// <summary>A source on a database named by a storage URI (<c>mysql://…</c>).</summary>
    /// <param name="uri">The database URI.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static MySqlSourceNode<T> Source<T>(StorageUri uri, string query, Func<MySqlReadOptions, MySqlReadOptions>? configure = null) =>
        new(Read(new MySqlReadOptions { Uri = uri, Query = query }, configure));

    /// <summary>A source on a connection from <paramref name="pool" />; set <see cref="MySqlReadOptions.ConnectionName" /> for a named one.</summary>
    /// <param name="pool">The connection pool.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static MySqlSourceNode<T> Source<T>(IMySqlConnectionPool pool, string query, Func<MySqlReadOptions, MySqlReadOptions>? configure = null) =>
        new(Read(new MySqlReadOptions { ConnectionPool = pool, Query = query }, configure));

    /// <summary>A sink that writes <typeparamref name="T" />'s readable members to <paramref name="table" />.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static MySqlSinkNode<T> Sink<T>(string connectionString, string table, Func<MySqlWriteOptions, MySqlWriteOptions>? configure = null) =>
        new(Write(new MySqlWriteOptions { ConnectionString = connectionString, Table = table }, configure));

    /// <summary>A sink on a database named by a storage URI (<c>mysql://…</c>).</summary>
    /// <param name="uri">The database URI.</param>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static MySqlSinkNode<T> Sink<T>(StorageUri uri, string table, Func<MySqlWriteOptions, MySqlWriteOptions>? configure = null) =>
        new(Write(new MySqlWriteOptions { Uri = uri, Table = table }, configure));

    /// <summary>A sink on a connection from <paramref name="pool" />; set <see cref="MySqlWriteOptions.ConnectionName" /> for a named one.</summary>
    /// <param name="pool">The connection pool.</param>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static MySqlSinkNode<T> Sink<T>(IMySqlConnectionPool pool, string table, Func<MySqlWriteOptions, MySqlWriteOptions>? configure = null) =>
        new(Write(new MySqlWriteOptions { ConnectionPool = pool, Table = table }, configure));

    private static MySqlReadOptions Read(MySqlReadOptions options, Func<MySqlReadOptions, MySqlReadOptions>? configure) =>
        configure is null ? options : configure(options);

    private static MySqlWriteOptions Write(MySqlWriteOptions options, Func<MySqlWriteOptions, MySqlWriteOptions>? configure) =>
        configure is null ? options : configure(options);
}
