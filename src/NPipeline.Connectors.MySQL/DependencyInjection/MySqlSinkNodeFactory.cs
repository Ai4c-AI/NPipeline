using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Connection;
using NPipeline.Connectors.MySql.Nodes;

namespace NPipeline.Connectors.MySql.DependencyInjection;

/// <summary>Creates MySQL sinks on the registered connection pool.</summary>
public interface IMySqlSinkNodeFactory
{
    /// <summary>A sink that writes <typeparamref name="T" />'s readable members to <paramref name="table" />.</summary>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options; set <see cref="MySqlWriteOptions.ConnectionName" /> for a named connection.</param>
    MySqlSinkNode<T> CreateSinkNode<T>(string table, Func<MySqlWriteOptions, MySqlWriteOptions>? configure = null);
}

/// <summary>Creates MySQL sinks on the registered connection pool.</summary>
/// <param name="connectionPool">The pool.</param>
public class MySqlSinkNodeFactory(IMySqlConnectionPool connectionPool) : IMySqlSinkNodeFactory
{
    private readonly IMySqlConnectionPool _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));

    /// <inheritdoc />
    public MySqlSinkNode<T> CreateSinkNode<T>(string table, Func<MySqlWriteOptions, MySqlWriteOptions>? configure = null)
    {
        var options = new MySqlWriteOptions { ConnectionPool = _connectionPool, Table = table };
        options = configure is null ? options : configure(options);

        if (options.ConnectionName is { Length: > 0 } name && !_connectionPool.HasNamedConnection(name))
            throw new InvalidOperationException($"Named connection '{name}' not found.");

        return new MySqlSinkNode<T>(options);
    }
}
