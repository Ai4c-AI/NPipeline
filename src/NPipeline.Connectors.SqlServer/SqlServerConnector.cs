using NPipeline.Connectors.Sql;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.Connectors.SqlServer.Nodes;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.SqlServer;

/// <summary>Creates SQL Server sources and sinks.</summary>
/// <example>
///     <code>
/// var source = SqlServerConnector.Source&lt;Order&gt;(connectionString, "SELECT * FROM dbo.Orders ORDER BY Id");
/// var sink = SqlServerConnector.Sink&lt;Order&gt;(connectionString, "Orders", o => o with { WriteStrategy = SqlServerWriteStrategy.BulkCopy });
///     </code>
/// </example>
public static class SqlServerConnector
{
    /// <summary>A source that runs <paramref name="query" /> and maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options, typically with a <c>with</c> expression.</param>
    public static SqlServerSourceNode<T> Source<T>(string connectionString, string query, Func<SqlServerReadOptions, SqlServerReadOptions>? configure = null) =>
        new(Read(new SqlServerReadOptions { ConnectionString = connectionString, Query = query }, configure));

    /// <summary>A source that runs <paramref name="query" /> and builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="query">The query.</param>
    /// <param name="map">Builds a record from a row.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static SqlServerSourceNode<T> Source<T>(string connectionString, string query, Func<SqlRow, T> map,
        Func<SqlServerReadOptions, SqlServerReadOptions>? configure = null) =>
        new(Read(new SqlServerReadOptions { ConnectionString = connectionString, Query = query }, configure), map ?? throw new ArgumentNullException(nameof(map)));

    /// <summary>A source on a database named by a storage URI (<c>mssql://…</c>).</summary>
    /// <param name="uri">The database URI.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static SqlServerSourceNode<T> Source<T>(StorageUri uri, string query, Func<SqlServerReadOptions, SqlServerReadOptions>? configure = null) =>
        new(Read(new SqlServerReadOptions { Uri = uri, Query = query }, configure));

    /// <summary>A source on a connection from <paramref name="pool" />; set <see cref="SqlServerReadOptions.ConnectionName" /> for a named one.</summary>
    /// <param name="pool">The connection pool.</param>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static SqlServerSourceNode<T> Source<T>(ISqlServerConnectionPool pool, string query, Func<SqlServerReadOptions, SqlServerReadOptions>? configure = null) =>
        new(Read(new SqlServerReadOptions { ConnectionPool = pool, Query = query }, configure));

    /// <summary>A sink that writes <typeparamref name="T" />'s readable members to <paramref name="table" />.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static SqlServerSinkNode<T> Sink<T>(string connectionString, string table, Func<SqlServerWriteOptions, SqlServerWriteOptions>? configure = null) =>
        new(Write(new SqlServerWriteOptions { ConnectionString = connectionString, Table = table }, configure));

    /// <summary>A sink on a database named by a storage URI (<c>mssql://…</c>).</summary>
    /// <param name="uri">The database URI.</param>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static SqlServerSinkNode<T> Sink<T>(StorageUri uri, string table, Func<SqlServerWriteOptions, SqlServerWriteOptions>? configure = null) =>
        new(Write(new SqlServerWriteOptions { Uri = uri, Table = table }, configure));

    /// <summary>A sink on a connection from <paramref name="pool" />; set <see cref="SqlServerWriteOptions.ConnectionName" /> for a named one.</summary>
    /// <param name="pool">The connection pool.</param>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static SqlServerSinkNode<T> Sink<T>(ISqlServerConnectionPool pool, string table, Func<SqlServerWriteOptions, SqlServerWriteOptions>? configure = null) =>
        new(Write(new SqlServerWriteOptions { ConnectionPool = pool, Table = table }, configure));

    private static SqlServerReadOptions Read(SqlServerReadOptions options, Func<SqlServerReadOptions, SqlServerReadOptions>? configure) =>
        configure is null ? options : configure(options);

    private static SqlServerWriteOptions Write(SqlServerWriteOptions options, Func<SqlServerWriteOptions, SqlServerWriteOptions>? configure) =>
        configure is null ? options : configure(options);
}
