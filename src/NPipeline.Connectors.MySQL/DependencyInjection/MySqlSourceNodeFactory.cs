using NPipeline.Connectors.Sql;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Connection;
using NPipeline.Connectors.MySql.Nodes;

namespace NPipeline.Connectors.MySql.DependencyInjection;

/// <summary>Creates MySQL sources on the registered connection pool.</summary>
public interface IMySqlSourceNodeFactory
{
    /// <summary>A source that maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options; set <see cref="MySqlReadOptions.ConnectionName" /> for a named connection.</param>
    MySqlSourceNode<T> CreateSourceNode<T>(string query, Func<MySqlReadOptions, MySqlReadOptions>? configure = null);

    /// <summary>A source that builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="query">The query.</param>
    /// <param name="map">Builds a record from a row.</param>
    /// <param name="configure">Adjusts the default options.</param>
    MySqlSourceNode<T> CreateSourceNode<T>(string query, Func<SqlRow, T> map, Func<MySqlReadOptions, MySqlReadOptions>? configure = null);
}

/// <summary>Creates MySQL sources on the registered connection pool.</summary>
/// <param name="connectionPool">The pool.</param>
public class MySqlSourceNodeFactory(IMySqlConnectionPool connectionPool) : IMySqlSourceNodeFactory
{
    private readonly IMySqlConnectionPool _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));

    /// <inheritdoc />
    public MySqlSourceNode<T> CreateSourceNode<T>(string query, Func<MySqlReadOptions, MySqlReadOptions>? configure = null) =>
        new(Options(query, configure));

    /// <inheritdoc />
    public MySqlSourceNode<T> CreateSourceNode<T>(string query, Func<SqlRow, T> map, Func<MySqlReadOptions, MySqlReadOptions>? configure = null) =>
        new(Options(query, configure), map ?? throw new ArgumentNullException(nameof(map)));

    private MySqlReadOptions Options(string query, Func<MySqlReadOptions, MySqlReadOptions>? configure)
    {
        var options = new MySqlReadOptions { ConnectionPool = _connectionPool, Query = query };
        options = configure is null ? options : configure(options);

        if (options.ConnectionName is { Length: > 0 } name && !_connectionPool.HasNamedConnection(name))
            throw new InvalidOperationException($"Named connection '{name}' not found.");

        return options;
    }
}
