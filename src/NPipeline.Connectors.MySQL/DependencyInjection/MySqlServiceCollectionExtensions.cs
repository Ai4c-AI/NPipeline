using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Connection;

namespace NPipeline.Connectors.MySql.DependencyInjection;

/// <summary>
///     Extension methods for configuring the MySQL connector in dependency injection. The methods share one
///     <see cref="MySqlOptions" /> instance, so they can be called in any order.
/// </summary>
public static class MySqlServiceCollectionExtensions
{
    /// <summary>
    ///     Adds the MySQL connector to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action, applied to the options connections were already added to.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddMySqlConnector(
        this IServiceCollection services,
        Action<MySqlOptions>? configure = null) =>
        services.AddMySqlConnector<MySqlOptions>(configure);

    /// <summary>
    ///     Adds the MySQL connector to the service collection with custom options. Connections added before this call
    ///     are carried over to the <typeparamref name="TOptions" /> instance.
    /// </summary>
    /// <typeparam name="TOptions">The options type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddMySqlConnector<TOptions>(
        this IServiceCollection services,
        Action<TOptions>? configure = null)
        where TOptions : MySqlOptions, new()
    {
        configure?.Invoke(Options<TOptions>(services));

        services.TryAddSingleton<IMySqlConnectionPool>(sp => new MySqlConnectionPool(sp.GetRequiredService<MySqlOptions>()));
        services.TryAddSingleton<IMySqlSourceNodeFactory, MySqlSourceNodeFactory>();
        services.TryAddSingleton<IMySqlSinkNodeFactory, MySqlSinkNodeFactory>();

        return services;
    }

    /// <summary>
    ///     Adds a named MySQL connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddMySqlConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        Options<MySqlOptions>(services).AddOrUpdateConnection(name, connectionString);
        return services;
    }

    /// <summary>
    ///     Adds the default MySQL connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDefaultMySqlConnection(
        this IServiceCollection services,
        string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        Options<MySqlOptions>(services).DefaultConnectionString = connectionString;
        return services;
    }

    /// <summary>
    ///     Adds a keyed MySQL connection pool to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name/key.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddKeyedMySqlConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        _ = services.AddKeyedSingleton<IMySqlConnectionPool>(name, (_, _) => new MySqlConnectionPool(connectionString));

        return services;
    }

    /// <summary>
    ///     The registered options, registering them first if needed. Options of a base type registered by an earlier call
    ///     are replaced by <typeparamref name="TOptions" /> with their connections copied, so nothing an earlier call set is lost.
    /// </summary>
    private static TOptions Options<TOptions>(IServiceCollection services)
        where TOptions : MySqlOptions, new()
    {
        var descriptor = services.FirstOrDefault(sd => sd.ServiceType == typeof(MySqlOptions));

        if (descriptor?.ImplementationInstance is TOptions existing)
            return existing;

        if (descriptor is not null && descriptor.ImplementationInstance is not MySqlOptions)
            throw new InvalidOperationException($"{nameof(MySqlOptions)} is registered with a factory or type; configure connections there instead.");

        var options = new TOptions();

        if (descriptor?.ImplementationInstance is MySqlOptions registered)
        {
            options.DefaultConnectionString = registered.DefaultConnectionString;

            foreach (var (name, connectionString) in registered.NamedConnections)
            {
                options.AddOrUpdateConnection(name, connectionString);
            }

            _ = services.Remove(descriptor);
        }

        _ = services.AddSingleton<MySqlOptions>(options);
        return options;
    }
}
