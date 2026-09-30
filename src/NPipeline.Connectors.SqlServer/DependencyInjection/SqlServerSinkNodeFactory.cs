using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.Connectors.SqlServer.Nodes;

namespace NPipeline.Connectors.SqlServer.DependencyInjection;

/// <summary>Creates SQL Server sinks on the registered connection pool.</summary>
public interface ISqlServerSinkNodeFactory
{
    /// <summary>A sink that writes <typeparamref name="T" />'s readable members to <paramref name="table" />.</summary>
    /// <param name="table">The table.</param>
    /// <param name="configure">Adjusts the default options; set <see cref="SqlServerWriteOptions.ConnectionName" /> for a named connection.</param>
    SqlServerSinkNode<T> CreateSinkNode<T>(string table, Func<SqlServerWriteOptions, SqlServerWriteOptions>? configure = null);
}

/// <summary>Creates SQL Server sinks on the registered connection pool.</summary>
/// <param name="connectionPool">The pool.</param>
public class SqlServerSinkNodeFactory(ISqlServerConnectionPool connectionPool) : ISqlServerSinkNodeFactory
{
    private readonly ISqlServerConnectionPool _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));

    /// <inheritdoc />
    public SqlServerSinkNode<T> CreateSinkNode<T>(string table, Func<SqlServerWriteOptions, SqlServerWriteOptions>? configure = null)
    {
        var options = new SqlServerWriteOptions { ConnectionPool = _connectionPool, Table = table };
        options = configure is null ? options : configure(options);

        if (options.ConnectionName is { Length: > 0 } name && _connectionPool is SqlServerConnectionPool pool && !pool.HasNamedConnection(name))
            throw new InvalidOperationException($"Named connection '{name}' not found.");

        return new SqlServerSinkNode<T>(options);
    }
}
