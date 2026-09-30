using MySqlConnector;
using NPipeline.Connectors.MySql.Configuration;

namespace NPipeline.Connectors.MySql.Connection;

/// <summary>
///     Manages MySQL connections using MySqlConnector's built-in connection pooling.
///     Supports a default connection and any number of named connections.
/// </summary>
internal sealed class MySqlConnectionPool : IMySqlConnectionPool
{
    private readonly Dictionary<string, string> _namedConnectionStrings;
    private bool _disposed;

    /// <summary>
    ///     Creates a pool backed by a single connection string.
    /// </summary>
    public MySqlConnectionPool(string connectionString)
    {
        ConnectionString = connectionString;
        _namedConnectionStrings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    ///     Creates a pool backed by named connection strings.
    /// </summary>
    public MySqlConnectionPool(IDictionary<string, string> namedConnections)
    {
        ConnectionString = null;

        _namedConnectionStrings = namedConnections
            .ToDictionary(kvp => kvp.Key, kvp => kvp.Value,
                StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    ///     Creates a pool from a <see cref="MySqlOptions" /> instance, injecting configuration overrides.
    /// </summary>
    public MySqlConnectionPool(MySqlOptions options)
    {
        ConnectionString = string.IsNullOrWhiteSpace(options.DefaultConnectionString) ? null : options.DefaultConnectionString;

        _namedConnectionStrings = options.NamedConnections
            .ToDictionary(
                kvp => kvp.Key,
                kvp => kvp.Value,
                StringComparer.OrdinalIgnoreCase);
    }

    /// <inheritdoc />
    public string? ConnectionString { get; }

    /// <inheritdoc />
    public async Task<MySqlConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (ConnectionString is null)
        {
            throw new InvalidOperationException("No default connection string is configured. Use a named connection instead.");
        }

        var connection = new MySqlConnection(ConnectionString);
        await OpenAsync(connection, cancellationToken).ConfigureAwait(false);
        return connection;
    }

    /// <inheritdoc />
    public async Task<MySqlConnection> GetConnectionAsync(string name,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_namedConnectionStrings.TryGetValue(name, out var cs))
        {
            throw new InvalidOperationException($"No connection with name '{name}' is configured.");
        }

        var connection = new MySqlConnection(cs);
        await OpenAsync(connection, cancellationToken).ConfigureAwait(false);
        return connection;
    }

    /// <inheritdoc />
    public bool HasNamedConnection(string name) => _namedConnectionStrings.ContainsKey(name);

    /// <inheritdoc />
    public IEnumerable<string> GetNamedConnectionNames() => _namedConnectionStrings.Keys;

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _disposed = true;

        // MySqlConnector manages pool lifecycle via static state; no instance cleanup needed here.
        return ValueTask.CompletedTask;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static async Task OpenAsync(MySqlConnection connection, CancellationToken ct)
    {
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            // The driver's exception is rethrown as it is, so the resilience classifier can judge it.
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
