using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Connection;
using NPipeline.Connectors.Snowflake.Nodes;

namespace NPipeline.Connectors.Snowflake.DependencyInjection;

/// <summary>Creates Snowflake sinks on the registered connection pool.</summary>
public interface ISnowflakeSinkNodeFactory
{
    /// <summary>A sink that writes <typeparamref name="T" />'s readable members to <paramref name="table" />.</summary>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options; set <see cref="SnowflakeWriteOptions.ConnectionName" /> for a named connection.</param>
    SnowflakeSinkNode<T> CreateSinkNode<T>(string table, Func<SnowflakeWriteOptions, SnowflakeWriteOptions>? configure = null);
}

/// <summary>Creates Snowflake sinks on the registered connection pool.</summary>
/// <param name="connectionPool">The pool.</param>
public class SnowflakeSinkNodeFactory(ISnowflakeConnectionPool connectionPool) : ISnowflakeSinkNodeFactory
{
    private readonly ISnowflakeConnectionPool _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));

    /// <inheritdoc />
    public SnowflakeSinkNode<T> CreateSinkNode<T>(string table, Func<SnowflakeWriteOptions, SnowflakeWriteOptions>? configure = null)
    {
        var options = new SnowflakeWriteOptions { ConnectionPool = _connectionPool, Table = table };
        options = configure is null ? options : configure(options);

        if (options.ConnectionName is { Length: > 0 } name && !_connectionPool.HasNamedConnection(name))
            throw new InvalidOperationException($"Named connection '{name}' not found.");

        return new SnowflakeSinkNode<T>(options);
    }
}
