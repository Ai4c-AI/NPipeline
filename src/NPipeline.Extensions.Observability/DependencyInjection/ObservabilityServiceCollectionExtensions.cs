using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Execution;
using NPipeline.Extensions.Observability;
using NPipeline.Observability.Configuration;
using NPipeline.Observability.Metrics;

namespace NPipeline.Observability.DependencyInjection;

/// <summary>
///     Provides extension methods for setting up NPipeline observability services in an <see cref="IServiceCollection" />.
/// </summary>
/// <remarks>
///     <para>
///         Metrics are recorded for runs whose context comes from the container: <c>RunPipelineAsync</c>,
///         <c>CreatePipelineContext</c> or <see cref="IObservablePipelineContextFactory" />. A run with a
///         <c>new PipelineContext()</c> records nothing and logs a warning.
///     </para>
///     <para>
///         Item counts are recorded only for nodes configured with <c>WithObservability</c>, unless
///         <see cref="ObservabilityExtensionOptions.AutoObserveAllNodes" /> is set.
///     </para>
///     <para>
///         The default logging sinks need logging registered (<c>services.AddLogging()</c> or a host); without it they log
///         to a null logger.
///     </para>
/// </remarks>
public static class ObservabilityServiceCollectionExtensions
{
    // The options an overload without an options parameter registers. Compared by reference: the caller did not choose
    // them, so a later call that passes options replaces them.
    private static readonly ObservabilityExtensionOptions ImplicitDefaultOptions = new();

    /// <summary>
    ///     Adds NPipeline observability services with default logging sinks.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        return services.AddNPipelineObservability<LoggingMetricsSink, LoggingPipelineMetricsSink>(ImplicitDefaultOptions);
    }

    /// <summary>
    ///     Adds NPipeline observability services with default logging sinks and custom configuration.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="options">Configuration options for the observability extension.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability(this IServiceCollection services, ObservabilityExtensionOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        return services.AddNPipelineObservability<LoggingMetricsSink, LoggingPipelineMetricsSink>(options);
    }

    /// <summary>
    ///     Adds NPipeline observability services with specified metrics sinks.
    /// </summary>
    /// <typeparam name="TMetricsSink">The type of the node metrics sink.</typeparam>
    /// <typeparam name="TPipelineMetricsSink">The type of the pipeline metrics sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability<TMetricsSink, TPipelineMetricsSink>(this IServiceCollection services)
        where TMetricsSink : class, IMetricsSink
        where TPipelineMetricsSink : class, IPipelineMetricsSink
    {
        ArgumentNullException.ThrowIfNull(services);

        return services.AddNPipelineObservability<TMetricsSink, TPipelineMetricsSink>(ImplicitDefaultOptions);
    }

    /// <summary>
    ///     Adds NPipeline observability services with specified metrics sinks and custom configuration.
    /// </summary>
    /// <typeparam name="TMetricsSink">The type of the node metrics sink.</typeparam>
    /// <typeparam name="TPipelineMetricsSink">The type of the pipeline metrics sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="options">Configuration options for the observability extension.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability<TMetricsSink, TPipelineMetricsSink>(
        this IServiceCollection services,
        ObservabilityExtensionOptions options)
        where TMetricsSink : class, IMetricsSink
        where TPipelineMetricsSink : class, IPipelineMetricsSink
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        // Register the observability collector as scoped (per pipeline run)
        services.TryAddScoped<IObservabilityCollector, ObservabilityCollector>();

        // Register the metrics sinks as scoped (per pipeline run)
        services.TryAddScoped<IMetricsSink, TMetricsSink>();
        services.TryAddScoped<IPipelineMetricsSink, TPipelineMetricsSink>();

        RegisterCoreObservabilityServices(services, options);

        return services;
    }

    /// <summary>
    ///     Adds NPipeline observability services using factory delegates for creating sinks.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="metricsSinkFactory">A factory delegate to create the node metrics sink.</param>
    /// <param name="pipelineMetricsSinkFactory">A factory delegate to create the pipeline metrics sink.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability(
        this IServiceCollection services,
        Func<IServiceProvider, IMetricsSink> metricsSinkFactory,
        Func<IServiceProvider, IPipelineMetricsSink> pipelineMetricsSinkFactory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(metricsSinkFactory);
        ArgumentNullException.ThrowIfNull(pipelineMetricsSinkFactory);

        return services.AddNPipelineObservability(metricsSinkFactory, pipelineMetricsSinkFactory, ImplicitDefaultOptions);
    }

    /// <summary>
    ///     Adds NPipeline observability services using factory delegates for creating sinks and custom configuration.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="metricsSinkFactory">A factory delegate to create the node metrics sink.</param>
    /// <param name="pipelineMetricsSinkFactory">A factory delegate to create the pipeline metrics sink.</param>
    /// <param name="options">Configuration options for the observability extension.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability(
        this IServiceCollection services,
        Func<IServiceProvider, IMetricsSink> metricsSinkFactory,
        Func<IServiceProvider, IPipelineMetricsSink> pipelineMetricsSinkFactory,
        ObservabilityExtensionOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(metricsSinkFactory);
        ArgumentNullException.ThrowIfNull(pipelineMetricsSinkFactory);
        ArgumentNullException.ThrowIfNull(options);

        // Register the observability collector as scoped (per pipeline run)
        services.TryAddScoped<IObservabilityCollector, ObservabilityCollector>();

        // Register the metrics sinks as scoped (per pipeline run) using factory delegates
        services.TryAddScoped(metricsSinkFactory);
        services.TryAddScoped<IPipelineMetricsSink>(pipelineMetricsSinkFactory);

        RegisterCoreObservabilityServices(services, options);

        return services;
    }

    /// <summary>
    ///     Adds NPipeline observability services with a custom collector implementation.
    /// </summary>
    /// <typeparam name="TObservabilityCollector">The type of the observability collector.</typeparam>
    /// <typeparam name="TMetricsSink">The type of the node metrics sink.</typeparam>
    /// <typeparam name="TPipelineMetricsSink">The type of the pipeline metrics sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability<TObservabilityCollector, TMetricsSink, TPipelineMetricsSink>(
        this IServiceCollection services)
        where TObservabilityCollector : class, IObservabilityCollector
        where TMetricsSink : class, IMetricsSink
        where TPipelineMetricsSink : class, IPipelineMetricsSink
    {
        ArgumentNullException.ThrowIfNull(services);

        return services.AddNPipelineObservability<TObservabilityCollector, TMetricsSink, TPipelineMetricsSink>(ImplicitDefaultOptions);
    }

    /// <summary>
    ///     Adds NPipeline observability services with a custom collector implementation and custom configuration.
    /// </summary>
    /// <typeparam name="TObservabilityCollector">The type of the observability collector.</typeparam>
    /// <typeparam name="TMetricsSink">The type of the node metrics sink.</typeparam>
    /// <typeparam name="TPipelineMetricsSink">The type of the pipeline metrics sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="options">Configuration options for the observability extension.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability<TObservabilityCollector, TMetricsSink, TPipelineMetricsSink>(
        this IServiceCollection services,
        ObservabilityExtensionOptions options)
        where TObservabilityCollector : class, IObservabilityCollector
        where TMetricsSink : class, IMetricsSink
        where TPipelineMetricsSink : class, IPipelineMetricsSink
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        // Register the custom observability collector
        services.TryAddScoped<IObservabilityCollector, TObservabilityCollector>();

        // Register the metrics sinks as scoped (per pipeline run)
        services.TryAddScoped<IMetricsSink, TMetricsSink>();
        services.TryAddScoped<IPipelineMetricsSink, TPipelineMetricsSink>();

        RegisterCoreObservabilityServices(services, options);

        return services;
    }

    /// <summary>
    ///     Adds NPipeline observability services with a custom collector implementation using a factory delegate.
    /// </summary>
    /// <typeparam name="TMetricsSink">The type of the node metrics sink.</typeparam>
    /// <typeparam name="TPipelineMetricsSink">The type of the pipeline metrics sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="collectorFactory">A factory delegate to create the observability collector.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability<TMetricsSink, TPipelineMetricsSink>(
        this IServiceCollection services,
        Func<IServiceProvider, IObservabilityCollector> collectorFactory)
        where TMetricsSink : class, IMetricsSink
        where TPipelineMetricsSink : class, IPipelineMetricsSink
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(collectorFactory);

        return services.AddNPipelineObservability<TMetricsSink, TPipelineMetricsSink>(collectorFactory, ImplicitDefaultOptions);
    }

    /// <summary>
    ///     Adds NPipeline observability services with a custom collector implementation using a factory delegate and custom configuration.
    /// </summary>
    /// <typeparam name="TMetricsSink">The type of the node metrics sink.</typeparam>
    /// <typeparam name="TPipelineMetricsSink">The type of the pipeline metrics sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="collectorFactory">A factory delegate to create the observability collector.</param>
    /// <param name="options">Configuration options for the observability extension.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability<TMetricsSink, TPipelineMetricsSink>(
        this IServiceCollection services,
        Func<IServiceProvider, IObservabilityCollector> collectorFactory,
        ObservabilityExtensionOptions options)
        where TMetricsSink : class, IMetricsSink
        where TPipelineMetricsSink : class, IPipelineMetricsSink
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(collectorFactory);
        ArgumentNullException.ThrowIfNull(options);

        // Register the observability collector using factory delegate
        services.TryAddScoped(collectorFactory);

        // Register the metrics sinks as scoped (per pipeline run)
        services.TryAddScoped<IMetricsSink, TMetricsSink>();
        services.TryAddScoped<IPipelineMetricsSink, TPipelineMetricsSink>();

        RegisterCoreObservabilityServices(services, options);

        return services;
    }

    /// <summary>
    ///     Changes the options the observability extension runs with, whether it is registered before or after this call.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Only the first <c>AddNPipelineObservability</c> call that passes options sets them, so a library or tool
    ///         that runs on top of an app's registration uses this to adjust the app's options instead of replacing them.
    ///         Changes apply in the order they are registered, on top of those options.
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    ///     services.ConfigureNPipelineObservability(options => options with { EnableMemoryMetrics = true });
    ///     </code>
    /// </example>
    /// <param name="services">The <see cref="IServiceCollection" /> to configure.</param>
    /// <param name="configure">Returns the options to use, given the options configured so far.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection ConfigureNPipelineObservability(
        this IServiceCollection services,
        Func<ObservabilityExtensionOptions, ObservabilityExtensionOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        _ = services.AddSingleton(new ObservabilityOptionsChange(configure));
        return services;
    }

    /// <summary>
    ///     Registers the shared core services used by all observability configurations.
    /// </summary>
    /// <remarks>
    ///     Safe to call more than once: every registration is added only if missing, so a later call never replaces the
    ///     app's options, observer or observability surface. The one exception is options a call without an options
    ///     parameter registered: the first call that passes options replaces them, so a library that registers
    ///     observability before the app does not hide the app's options.
    /// </remarks>
    private static void RegisterCoreObservabilityServices(IServiceCollection services, ObservabilityExtensionOptions options)
    {
        // The first chosen options (or the defaults, until a call chooses some), plus any ConfigureNPipelineObservability
        // changes, resolved once per container.
        RegisterOptions(services, options);
        services.TryAddSingleton(sp => sp.GetServices<ObservabilityOptionsChange>()
            .Aggregate(sp.GetRequiredService<ObservabilityOptionsBase>().Options, static (current, change) => change.Apply(current)));

        // Register the factory for DI resolution
        services.TryAddScoped<IObservabilityFactory, DiObservabilityFactory>();

        // The observer that bridges core events to the collector. Added alongside any observer the app registered:
        // every registered observer is notified.
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IExecutionObserver, MetricsCollectingExecutionObserver>(sp =>
            new MetricsCollectingExecutionObserver(
                sp.GetRequiredService<IObservabilityCollector>(),
                sp.GetRequiredService<ObservabilityExtensionOptions>().EnableMemoryMetrics)));

        // Register the context factory for automatic observer configuration
        services.TryAddScoped<IObservablePipelineContextFactory, ObservablePipelineContextFactory>();

        // Replace the core null observability surface with the real one, but keep a surface the app registered.
        var surface = services.LastOrDefault(static d => d.ServiceType == typeof(IObservabilitySurface));

        if (surface is null || surface.ImplementationInstance is NullObservabilitySurface)
        {
            _ = services.RemoveAll<IObservabilitySurface>();

            services.AddScoped<IObservabilitySurface>(static sp => new ObservabilitySurface(
                sp.GetRequiredService<ObservabilityExtensionOptions>().AutoObserveAllNodes
                    ? ObservabilityOptions.Default
                    : null));
        }
    }

    private static void RegisterOptions(IServiceCollection services, ObservabilityExtensionOptions options)
    {
        var chosen = !ReferenceEquals(options, ImplicitDefaultOptions);
        var existing = services.FirstOrDefault(static d => d.ServiceType == typeof(ObservabilityOptionsBase));

        if (existing is not null)
        {
            if (!chosen || existing.ImplementationInstance is ObservabilityOptionsBase { Chosen: true })
                return;

            _ = services.Remove(existing);
        }

        services.AddSingleton(new ObservabilityOptionsBase(options, chosen));
    }

    private sealed record ObservabilityOptionsBase(ObservabilityExtensionOptions Options, bool Chosen);

    private sealed record ObservabilityOptionsChange(Func<ObservabilityExtensionOptions, ObservabilityExtensionOptions> Apply);
}
