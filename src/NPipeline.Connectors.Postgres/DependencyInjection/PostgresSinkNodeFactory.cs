using NPipeline.Connectors.Postgres.Configuration;
using NPipeline.Connectors.Postgres.Connection;
using NPipeline.Connectors.Postgres.Nodes;

namespace NPipeline.Connectors.Postgres.DependencyInjection;

/// <summary>Creates PostgreSQL sinks on the registered connection pool.</summary>
public interface IPostgresSinkNodeFactory
{
    /// <summary>A sink that writes <typeparamref name="T" />'s readable members to <paramref name="table" />.</summary>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options; set <see cref="PostgresWriteOptions.ConnectionName" /> for a named connection.</param>
    PostgresSinkNode<T> CreateSinkNode<T>(string table, Func<PostgresWriteOptions, PostgresWriteOptions>? configure = null);
}

/// <summary>Creates PostgreSQL sinks on the registered connection pool.</summary>
/// <param name="connectionPool">The pool.</param>
public class PostgresSinkNodeFactory(IPostgresConnectionPool connectionPool) : IPostgresSinkNodeFactory
{
    private readonly IPostgresConnectionPool _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));

    /// <inheritdoc />
    public PostgresSinkNode<T> CreateSinkNode<T>(string table, Func<PostgresWriteOptions, PostgresWriteOptions>? configure = null)
    {
        var options = new PostgresWriteOptions { ConnectionPool = _connectionPool, Table = table };
        options = configure is null ? options : configure(options);

        if (options.ConnectionName is { Length: > 0 } name && !_connectionPool.HasNamedConnection(name))
            throw new InvalidOperationException($"Named connection '{name}' not found.");

        return new PostgresSinkNode<T>(options);
    }
}
