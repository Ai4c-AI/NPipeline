using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Connection;

namespace NPipeline.Connectors.SqlServer.DependencyInjection;

/// <summary>
///     Extension methods for configuring the SQL Server connector in dependency injection. The methods share one
///     <see cref="SqlServerOptions" /> instance, so they can be called in any order.
/// </summary>
public static class SqlServerServiceCollectionExtensions
{
    /// <summary>
    ///     Adds the SQL Server connector to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action, applied to the options connections were already added to.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlServerConnector(
        this IServiceCollection services,
        Action<SqlServerOptions>? configure = null) =>
        services.AddSqlServerConnector<SqlServerOptions>(configure);

    /// <summary>
    ///     Adds the SQL Server connector to the service collection with custom options. Connections added before this call
    ///     are carried over to the <typeparamref name="TOptions" /> instance.
    /// </summary>
    /// <typeparam name="TOptions">The options type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlServerConnector<TOptions>(
        this IServiceCollection services,
        Action<TOptions>? configure = null)
        where TOptions : SqlServerOptions, new()
    {
        configure?.Invoke(Options<TOptions>(services));

        services.TryAddSingleton<ISqlServerConnectionPool>(sp => new SqlServerConnectionPool(sp.GetRequiredService<SqlServerOptions>()));
        services.TryAddSingleton<ISqlServerSourceNodeFactory, SqlServerSourceNodeFactory>();
        services.TryAddSingleton<ISqlServerSinkNodeFactory, SqlServerSinkNodeFactory>();

        return services;
    }

    /// <summary>
    ///     Adds a named SQL Server connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlServerConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        Options<SqlServerOptions>(services).AddOrUpdateConnection(name, connectionString);
        return services;
    }

    /// <summary>
    ///     Adds the default SQL Server connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDefaultSqlServerConnection(
        this IServiceCollection services,
        string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        Options<SqlServerOptions>(services).DefaultConnectionString = connectionString;
        return services;
    }

    /// <summary>
    ///     Adds a keyed SQL Server connection pool to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name/key.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddKeyedSqlServerConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        _ = services.AddKeyedSingleton<ISqlServerConnectionPool>(name, (_, _) => new SqlServerConnectionPool(connectionString));

        return services;
    }

    /// <summary>
    ///     The registered options, registering them first if needed. Options of a base type registered by an earlier call
    ///     are replaced by <typeparamref name="TOptions" /> with their connections copied, so nothing an earlier call set is lost.
    /// </summary>
    private static TOptions Options<TOptions>(IServiceCollection services)
        where TOptions : SqlServerOptions, new()
    {
        var descriptor = services.FirstOrDefault(sd => sd.ServiceType == typeof(SqlServerOptions));

        if (descriptor?.ImplementationInstance is TOptions existing)
            return existing;

        if (descriptor is not null && descriptor.ImplementationInstance is not SqlServerOptions)
            throw new InvalidOperationException($"{nameof(SqlServerOptions)} is registered with a factory or type; configure connections there instead.");

        var options = new TOptions();

        if (descriptor?.ImplementationInstance is SqlServerOptions registered)
        {
            options.DefaultConnectionString = registered.DefaultConnectionString;

            foreach (var (name, connectionString) in registered.NamedConnections)
            {
                options.AddOrUpdateConnection(name, connectionString);
            }

            _ = services.Remove(descriptor);
        }

        _ = services.AddSingleton<SqlServerOptions>(options);
        return options;
    }
}
