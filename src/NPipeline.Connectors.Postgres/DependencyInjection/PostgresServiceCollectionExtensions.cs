using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.Postgres.Configuration;
using NPipeline.Connectors.Postgres.Connection;

namespace NPipeline.Connectors.Postgres.DependencyInjection;

/// <summary>
///     Extension methods for configuring the PostgreSQL connector in dependency injection. The methods share one
///     <see cref="PostgresOptions" /> instance, so they can be called in any order.
/// </summary>
public static class PostgresServiceCollectionExtensions
{
    /// <summary>
    ///     Adds the PostgreSQL connector to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action, applied to the options connections were already added to.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddPostgresConnector(
        this IServiceCollection services,
        Action<PostgresOptions>? configure = null) =>
        services.AddPostgresConnector<PostgresOptions>(configure);

    /// <summary>
    ///     Adds the PostgreSQL connector to the service collection with custom options. Connections added before this call
    ///     are carried over to the <typeparamref name="TOptions" /> instance.
    /// </summary>
    /// <typeparam name="TOptions">The options type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddPostgresConnector<TOptions>(
        this IServiceCollection services,
        Action<TOptions>? configure = null)
        where TOptions : PostgresOptions, new()
    {
        configure?.Invoke(Options<TOptions>(services));

        services.TryAddSingleton<IPostgresConnectionPool>(sp => new PostgresConnectionPool(sp.GetRequiredService<PostgresOptions>()));
        services.TryAddSingleton<IPostgresSourceNodeFactory, PostgresSourceNodeFactory>();
        services.TryAddSingleton<IPostgresSinkNodeFactory, PostgresSinkNodeFactory>();

        return services;
    }

    /// <summary>
    ///     Adds a named PostgreSQL connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddPostgresConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        Options<PostgresOptions>(services).AddOrUpdateConnection(name, connectionString);
        return services;
    }

    /// <summary>
    ///     Adds the default PostgreSQL connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDefaultPostgresConnection(
        this IServiceCollection services,
        string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        Options<PostgresOptions>(services).DefaultConnectionString = connectionString;
        return services;
    }

    /// <summary>
    ///     Adds a keyed PostgreSQL connection pool to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name/key.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddKeyedPostgresConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        _ = services.AddKeyedSingleton<IPostgresConnectionPool>(name, (_, _) => new PostgresConnectionPool(connectionString));

        return services;
    }

    /// <summary>
    ///     The registered options, registering them first if needed. Options of a base type registered by an earlier call
    ///     are replaced by <typeparamref name="TOptions" /> with their connections copied, so nothing an earlier call set is lost.
    /// </summary>
    private static TOptions Options<TOptions>(IServiceCollection services)
        where TOptions : PostgresOptions, new()
    {
        var descriptor = services.FirstOrDefault(sd => sd.ServiceType == typeof(PostgresOptions));

        if (descriptor?.ImplementationInstance is TOptions existing)
            return existing;

        if (descriptor is not null && descriptor.ImplementationInstance is not PostgresOptions)
            throw new InvalidOperationException($"{nameof(PostgresOptions)} is registered with a factory or type; configure connections there instead.");

        var options = new TOptions();

        if (descriptor?.ImplementationInstance is PostgresOptions registered)
        {
            options.DefaultConnectionString = registered.DefaultConnectionString;

            foreach (var (name, connectionString) in registered.NamedConnections)
            {
                options.AddOrUpdateConnection(name, connectionString);
            }

            _ = services.Remove(descriptor);
        }

        _ = services.AddSingleton<PostgresOptions>(options);
        return options;
    }
}
