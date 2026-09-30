using NPipeline.Connectors.Sql;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.Connectors.SqlServer.Nodes;

namespace NPipeline.Connectors.SqlServer.DependencyInjection;

/// <summary>Creates SQL Server sources on the registered connection pool.</summary>
public interface ISqlServerSourceNodeFactory
{
    /// <summary>A source that maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options; set <see cref="SqlServerReadOptions.ConnectionName" /> for a named connection.</param>
    SqlServerSourceNode<T> CreateSourceNode<T>(string query, Func<SqlServerReadOptions, SqlServerReadOptions>? configure = null);

    /// <summary>A source that builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="query">The query.</param>
    /// <param name="map">Builds a record from a row.</param>
    /// <param name="configure">Adjusts the default options.</param>
    SqlServerSourceNode<T> CreateSourceNode<T>(string query, Func<SqlRow, T> map, Func<SqlServerReadOptions, SqlServerReadOptions>? configure = null);
}

/// <summary>Creates SQL Server sources on the registered connection pool.</summary>
/// <param name="connectionPool">The pool.</param>
public class SqlServerSourceNodeFactory(ISqlServerConnectionPool connectionPool) : ISqlServerSourceNodeFactory
{
    private readonly ISqlServerConnectionPool _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));

    /// <inheritdoc />
    public SqlServerSourceNode<T> CreateSourceNode<T>(string query, Func<SqlServerReadOptions, SqlServerReadOptions>? configure = null) =>
        new(Options(query, configure));

    /// <inheritdoc />
    public SqlServerSourceNode<T> CreateSourceNode<T>(string query, Func<SqlRow, T> map, Func<SqlServerReadOptions, SqlServerReadOptions>? configure = null) =>
        new(Options(query, configure), map ?? throw new ArgumentNullException(nameof(map)));

    private SqlServerReadOptions Options(string query, Func<SqlServerReadOptions, SqlServerReadOptions>? configure)
    {
        var options = new SqlServerReadOptions { ConnectionPool = _connectionPool, Query = query };
        options = configure is null ? options : configure(options);

        if (options.ConnectionName is { Length: > 0 } name && _connectionPool is SqlServerConnectionPool pool && !pool.HasNamedConnection(name))
            throw new InvalidOperationException($"Named connection '{name}' not found.");

        return options;
    }
}
