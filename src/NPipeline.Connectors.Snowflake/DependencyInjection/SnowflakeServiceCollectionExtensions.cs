using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Connection;

namespace NPipeline.Connectors.Snowflake.DependencyInjection;

/// <summary>
///     Extension methods for configuring the Snowflake connector in dependency injection. The methods share one
///     <see cref="SnowflakeOptions" /> instance, so they can be called in any order.
/// </summary>
public static class SnowflakeServiceCollectionExtensions
{
    /// <summary>
    ///     Adds the Snowflake connector to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action, applied to the options connections were already added to.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSnowflakeConnector(
        this IServiceCollection services,
        Action<SnowflakeOptions>? configure = null) =>
        services.AddSnowflakeConnector<SnowflakeOptions>(configure);

    /// <summary>
    ///     Adds the Snowflake connector to the service collection with custom options. Connections added before this call
    ///     are carried over to the <typeparamref name="TOptions" /> instance.
    /// </summary>
    /// <typeparam name="TOptions">The options type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSnowflakeConnector<TOptions>(
        this IServiceCollection services,
        Action<TOptions>? configure = null)
        where TOptions : SnowflakeOptions, new()
    {
        configure?.Invoke(Options<TOptions>(services));

        services.TryAddSingleton<ISnowflakeConnectionPool>(sp => new SnowflakeConnectionPool(sp.GetRequiredService<SnowflakeOptions>()));
        services.TryAddSingleton<ISnowflakeSourceNodeFactory, SnowflakeSourceNodeFactory>();
        services.TryAddSingleton<ISnowflakeSinkNodeFactory, SnowflakeSinkNodeFactory>();

        return services;
    }

    /// <summary>
    ///     Adds a named Snowflake connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSnowflakeConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        Options<SnowflakeOptions>(services).AddOrUpdateConnection(name, connectionString);
        return services;
    }

    /// <summary>
    ///     Adds the default Snowflake connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDefaultSnowflakeConnection(
        this IServiceCollection services,
        string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        Options<SnowflakeOptions>(services).DefaultConnectionString = connectionString;
        return services;
    }

    /// <summary>
    ///     Adds a keyed Snowflake connection pool to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name/key.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddKeyedSnowflakeConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        _ = services.AddKeyedSingleton<ISnowflakeConnectionPool>(name, (_, _) => new SnowflakeConnectionPool(connectionString));

        return services;
    }

    /// <summary>
    ///     The registered options, registering them first if needed. Options of a base type registered by an earlier call
    ///     are replaced by <typeparamref name="TOptions" /> with their connections copied, so nothing an earlier call set is lost.
    /// </summary>
    private static TOptions Options<TOptions>(IServiceCollection services)
        where TOptions : SnowflakeOptions, new()
    {
        var descriptor = services.FirstOrDefault(sd => sd.ServiceType == typeof(SnowflakeOptions));

        if (descriptor?.ImplementationInstance is TOptions existing)
            return existing;

        if (descriptor is not null && descriptor.ImplementationInstance is not SnowflakeOptions)
            throw new InvalidOperationException($"{nameof(SnowflakeOptions)} is registered with a factory or type; configure connections there instead.");

        var options = new TOptions();

        if (descriptor?.ImplementationInstance is SnowflakeOptions registered)
        {
            options.DefaultConnectionString = registered.DefaultConnectionString;

            foreach (var (name, connectionString) in registered.NamedConnections)
            {
                options.AddOrUpdateConnection(name, connectionString);
            }

            _ = services.Remove(descriptor);
        }

        _ = services.AddSingleton<SnowflakeOptions>(options);
        return options;
    }
}
