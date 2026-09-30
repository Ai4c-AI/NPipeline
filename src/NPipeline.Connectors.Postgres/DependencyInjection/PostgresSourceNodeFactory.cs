using NPipeline.Connectors.Sql;
using NPipeline.Connectors.Postgres.Configuration;
using NPipeline.Connectors.Postgres.Connection;
using NPipeline.Connectors.Postgres.Nodes;

namespace NPipeline.Connectors.Postgres.DependencyInjection;

/// <summary>Creates PostgreSQL sources on the registered connection pool.</summary>
public interface IPostgresSourceNodeFactory
{
    /// <summary>A source that maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <param name="query">The query.</param>
    /// <param name="configure">Adjusts the default options; set <see cref="PostgresReadOptions.ConnectionName" /> for a named connection.</param>
    PostgresSourceNode<T> CreateSourceNode<T>(string query, Func<PostgresReadOptions, PostgresReadOptions>? configure = null);

    /// <summary>A source that builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="query">The query.</param>
    /// <param name="map">Builds a record from a row.</param>
    /// <param name="configure">Adjusts the default options.</param>
    PostgresSourceNode<T> CreateSourceNode<T>(string query, Func<SqlRow, T> map, Func<PostgresReadOptions, PostgresReadOptions>? configure = null);
}

/// <summary>Creates PostgreSQL sources on the registered connection pool.</summary>
/// <param name="connectionPool">The pool.</param>
public class PostgresSourceNodeFactory(IPostgresConnectionPool connectionPool) : IPostgresSourceNodeFactory
{
    private readonly IPostgresConnectionPool _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));

    /// <inheritdoc />
    public PostgresSourceNode<T> CreateSourceNode<T>(string query, Func<PostgresReadOptions, PostgresReadOptions>? configure = null) =>
        new(Options(query, configure));

    /// <inheritdoc />
    public PostgresSourceNode<T> CreateSourceNode<T>(string query, Func<SqlRow, T> map, Func<PostgresReadOptions, PostgresReadOptions>? configure = null) =>
        new(Options(query, configure), map ?? throw new ArgumentNullException(nameof(map)));

    private PostgresReadOptions Options(string query, Func<PostgresReadOptions, PostgresReadOptions>? configure)
    {
        var options = new PostgresReadOptions { ConnectionPool = _connectionPool, Query = query };
        options = configure is null ? options : configure(options);

        if (options.ConnectionName is { Length: > 0 } name && !_connectionPool.HasNamedConnection(name))
            throw new InvalidOperationException($"Named connection '{name}' not found.");

        return options;
    }
}
