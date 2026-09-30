using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Nodes;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.DuckDB.DependencyInjection;

/// <summary>Creates DuckDB sources on the registered databases.</summary>
/// <param name="options">The registered databases.</param>
public class DuckDBSourceNodeFactory(DuckDBOptions options)
{
    private readonly DuckDBOptions _options = options ?? throw new ArgumentNullException(nameof(options));

    /// <summary>A source on the named (or default) database that maps columns to members by name.</summary>
    public DuckDBSourceNode<T> CreateSource<T>(string query, string? databaseName = null, Func<DuckDBReadOptions, DuckDBReadOptions>? configure = null) =>
        DuckDBConnector.Source<T>(_options.GetDatabase(databaseName), query, configure);

    /// <summary>A source on the named (or default) database that builds records with <paramref name="map" />.</summary>
    public DuckDBSourceNode<T> CreateSource<T>(string query, Func<SqlRow, T> map, string? databaseName = null,
        Func<DuckDBReadOptions, DuckDBReadOptions>? configure = null) =>
        DuckDBConnector.Source(_options.GetDatabase(databaseName), query, map, configure);
}
