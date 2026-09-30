using NPipeline.Connectors.Sql;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Connection;
using NPipeline.Connectors.Snowflake.Nodes;

namespace NPipeline.Connectors.Snowflake.DependencyInjection;

/// <summary>Creates Snowflake sources on the registered connection pool.</summary>
public interface ISnowflakeSourceNodeFactory
{
    /// <summary>A source that maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options; set <see cref="SnowflakeReadOptions.ConnectionName" /> for a named connection.</param>
    SnowflakeSourceNode<T> CreateSourceNode<T>(string query, Func<SnowflakeReadOptions, SnowflakeReadOptions>? configure = null);

    /// <summary>A source that builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="query">The query.</param>
    /// <param name="map">Builds a record from a row.</param>
    /// <param name="configure">Adjusts the default options.</param>
    SnowflakeSourceNode<T> CreateSourceNode<T>(string query, Func<SqlRow, T> map, Func<SnowflakeReadOptions, SnowflakeReadOptions>? configure = null);
}

/// <summary>Creates Snowflake sources on the registered connection pool.</summary>
/// <param name="connectionPool">The pool.</param>
public class SnowflakeSourceNodeFactory(ISnowflakeConnectionPool connectionPool) : ISnowflakeSourceNodeFactory
{
    private readonly ISnowflakeConnectionPool _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));

    /// <inheritdoc />
    public SnowflakeSourceNode<T> CreateSourceNode<T>(string query, Func<SnowflakeReadOptions, SnowflakeReadOptions>? configure = null) =>
        new(Options(query, configure));

    /// <inheritdoc />
    public SnowflakeSourceNode<T> CreateSourceNode<T>(string query, Func<SqlRow, T> map, Func<SnowflakeReadOptions, SnowflakeReadOptions>? configure = null) =>
        new(Options(query, configure), map ?? throw new ArgumentNullException(nameof(map)));

    private SnowflakeReadOptions Options(string query, Func<SnowflakeReadOptions, SnowflakeReadOptions>? configure)
    {
        var options = new SnowflakeReadOptions { ConnectionPool = _connectionPool, Query = query };
        options = configure is null ? options : configure(options);

        if (options.ConnectionName is { Length: > 0 } name && !_connectionPool.HasNamedConnection(name))
            throw new InvalidOperationException($"Named connection '{name}' not found.");

        return options;
    }
}
