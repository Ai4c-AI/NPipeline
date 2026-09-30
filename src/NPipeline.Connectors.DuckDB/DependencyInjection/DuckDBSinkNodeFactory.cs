using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Nodes;

namespace NPipeline.Connectors.DuckDB.DependencyInjection;

/// <summary>Creates DuckDB sinks on the registered databases.</summary>
/// <param name="options">The registered databases.</param>
public class DuckDBSinkNodeFactory(DuckDBOptions options)
{
    private readonly DuckDBOptions _options = options ?? throw new ArgumentNullException(nameof(options));

    /// <summary>A sink that writes to <paramref name="table" /> in the named (or default) database.</summary>
    public DuckDBSinkNode<T> CreateSink<T>(string table, string? databaseName = null, Func<DuckDBWriteOptions, DuckDBWriteOptions>? configure = null) =>
        DuckDBConnector.Sink<T>(_options.GetDatabase(databaseName), table, configure);
}
