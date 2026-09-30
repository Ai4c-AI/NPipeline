namespace NPipeline.Connectors.DuckDB.Configuration;

/// <summary>The DuckDB databases registered with dependency injection: a default and any number of named ones.</summary>
public sealed class DuckDBOptions
{
    /// <summary>The named databases.</summary>
    public Dictionary<string, DuckDBDatabase> NamedDatabases { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>The default database. Defaults to an in-memory database.</summary>
    public DuckDBDatabase DefaultDatabase { get; set; } = DuckDBDatabase.InMemory;

    /// <summary>The named database, or the default when <paramref name="name" /> is empty.</summary>
    /// <exception cref="InvalidOperationException">There is no such database.</exception>
    public DuckDBDatabase GetDatabase(string? name = null)
    {
        if (string.IsNullOrWhiteSpace(name))
            return DefaultDatabase;

        return NamedDatabases.TryGetValue(name, out var database)
            ? database
            : throw new InvalidOperationException($"Named database '{name}' not found.");
    }

    /// <summary>Whether a database with this name is registered.</summary>
    public bool HasDatabase(string name) => !string.IsNullOrWhiteSpace(name) && NamedDatabases.ContainsKey(name);
}
