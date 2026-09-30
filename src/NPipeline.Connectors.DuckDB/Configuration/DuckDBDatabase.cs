using System.Data.Common;
using System.Globalization;
using System.Text.RegularExpressions;
using DuckDB.NET.Data;

namespace NPipeline.Connectors.DuckDB.Configuration;

/// <summary>
///     A DuckDB database and how to open it: the file (or an in-memory database), the access mode, and settings applied to
///     each connection.
/// </summary>
/// <remarks>
///     Each connection to an in-memory database opens a database of its own, so a source cannot read what a sink wrote to
///     one; use a file to share data between nodes.
/// </remarks>
public sealed partial record DuckDBDatabase
{
    /// <summary>An in-memory database.</summary>
    public static DuckDBDatabase InMemory { get; } = new();

    /// <summary>The database file; <c>null</c> for an in-memory database.</summary>
    public string? Path { get; init; }

    /// <summary>How the file is opened. Defaults to <see cref="DuckDBAccessMode.Automatic" />.</summary>
    public DuckDBAccessMode AccessMode { get; init; } = DuckDBAccessMode.Automatic;

    /// <summary>DuckDB's <c>memory_limit</c>, such as <c>4GB</c>; <c>null</c> keeps DuckDB's default.</summary>
    public string? MemoryLimit { get; init; }

    /// <summary>DuckDB's <c>threads</c>; 0 keeps DuckDB's default.</summary>
    public int Threads { get; init; }

    /// <summary>DuckDB's <c>temp_directory</c>; <c>null</c> keeps DuckDB's default.</summary>
    public string? TempDirectory { get; init; }

    /// <summary>Extensions installed and loaded on each connection, such as <c>httpfs</c>.</summary>
    public IReadOnlyList<string> Extensions { get; init; } = [];

    /// <summary>Other settings applied with <c>SET name = 'value'</c> on each connection.</summary>
    public IReadOnlyDictionary<string, string> Settings { get; init; } = new Dictionary<string, string>();

    /// <summary>A database file.</summary>
    public static DuckDBDatabase File(string path) => new() { Path = path ?? throw new ArgumentNullException(nameof(path)) };

    /// <summary>The ADO.NET connection string.</summary>
    public string ConnectionString =>
        string.IsNullOrWhiteSpace(Path)
            ? "DataSource=:memory:"
            : AccessMode switch
            {
                DuckDBAccessMode.ReadOnly => $"DataSource={Path};access_mode=READ_ONLY",
                DuckDBAccessMode.ReadWrite => $"DataSource={Path};access_mode=READ_WRITE",
                _ => $"DataSource={Path}",
            };

    /// <summary>Throws if a setting or extension name is not a plain identifier.</summary>
    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegative(Threads, nameof(Threads));

        foreach (var name in Extensions.Concat(Settings.Keys))
        {
            if (!Identifier().IsMatch(name))
                throw new ArgumentException($"'{name}' is not a valid DuckDB extension or setting name.", nameof(Settings));
        }
    }

    /// <summary>Opens a connection and applies the extensions and settings.</summary>
    internal async Task<DbConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new DuckDBConnection(ConnectionString);

        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            foreach (var statement in SetupStatements())
            {
                var command = connection.CreateCommand();

                await using (command.ConfigureAwait(false))
                {
                    command.CommandText = statement;
                    _ = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private IEnumerable<string> SetupStatements()
    {
        foreach (var extension in Extensions)
        {
            yield return $"INSTALL {extension}; LOAD {extension};";
        }

        if (MemoryLimit is not null)
            yield return $"SET memory_limit = {Literal(MemoryLimit)}";

        if (Threads > 0)
            yield return $"SET threads = {Threads.ToString(CultureInfo.InvariantCulture)}";

        if (TempDirectory is not null)
            yield return $"SET temp_directory = {Literal(TempDirectory)}";

        foreach (var (name, value) in Settings)
        {
            yield return $"SET {name} = {Literal(value)}";
        }
    }

    private static string Literal(string value) => $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex Identifier();
}
