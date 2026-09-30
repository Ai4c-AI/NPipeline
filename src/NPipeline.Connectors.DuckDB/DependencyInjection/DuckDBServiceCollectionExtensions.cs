using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.DuckDB.Configuration;

namespace NPipeline.Connectors.DuckDB.DependencyInjection;

/// <summary>Registers the DuckDB connector and its databases.</summary>
public static class DuckDBServiceCollectionExtensions
{
    /// <summary>Registers the node factories and the database options.</summary>
    public static IServiceCollection AddDuckDBConnector(this IServiceCollection services, Action<DuckDBOptions>? configure = null)
    {
        configure?.Invoke(Options(services));
        services.TryAddSingleton<DuckDBSourceNodeFactory>();
        services.TryAddSingleton<DuckDBSinkNodeFactory>();
        return services;
    }

    /// <summary>Registers a named database.</summary>
    public static IServiceCollection AddDuckDBDatabase(this IServiceCollection services, string name, DuckDBDatabase database)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(database);
        Options(services).NamedDatabases[name] = database;
        return services;
    }

    /// <summary>Sets the default database.</summary>
    public static IServiceCollection AddDefaultDuckDBDatabase(this IServiceCollection services, DuckDBDatabase database)
    {
        ArgumentNullException.ThrowIfNull(database);
        Options(services).DefaultDatabase = database;
        return services;
    }

    private static DuckDBOptions Options(IServiceCollection services)
    {
        if (services.FirstOrDefault(sd => sd.ServiceType == typeof(DuckDBOptions))?.ImplementationInstance is DuckDBOptions existing)
            return existing;

        var options = new DuckDBOptions();
        services.TryAddSingleton(options);
        return options;
    }
}
