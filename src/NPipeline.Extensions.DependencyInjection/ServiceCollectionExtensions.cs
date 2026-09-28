using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Services;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Extensions.DependencyInjection;

/// <summary>
///     Provides extension methods for setting up NPipeline services in an <see cref="IServiceCollection" />.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Adds NPipeline services and configures them using a fluent API.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="configure">A delegate to configure the NPipeline services.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipeline(this IServiceCollection services,
        Action<NPipelineServiceBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        // Register Core Services (per-run scoped where appropriate)
        RegisterCoreServices(services);

        // Create and register the pipeline definition registry
        var registry = new PipelineDefinitionRegistry();
        services.TryAddSingleton(registry);

        // Configure using the fluent builder
        var builder = new NPipelineServiceBuilder(services, registry);
        configure(builder);

        return services;
    }

    /// <summary>
    ///     Scans the specified assemblies for pipeline components and registers them with the service collection.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="assembliesToScan">The assemblies to scan for nodes and definitions.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipeline(this IServiceCollection services, params Assembly[] assembliesToScan)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(assembliesToScan);

        // Register Core Services (per-run scoped where appropriate)
        RegisterCoreServices(services);

        // Register the pipeline definition registry
        var registry = new PipelineDefinitionRegistry();
        services.TryAddSingleton(registry);

        // Register Nodes and Definitions (safe assembly scanning)
        var typesToRegister = assembliesToScan
            .SelectMany(GetLoadableTypes)
            .Where(t => t is { IsClass: true, IsAbstract: false } &&
                        (typeof(INode).IsAssignableFrom(t) ||
                         typeof(IPipelineDefinition).IsAssignableFrom(t) ||
                         typeof(IResiliencePolicy).IsAssignableFrom(t) ||
                         typeof(IDeadLetterSink).IsAssignableFrom(t) ||
                         typeof(ILineageSink).IsAssignableFrom(t) ||
                         typeof(IPipelineLineageSink).IsAssignableFrom(t) ||
                         typeof(IPipelineLineageSinkProvider).IsAssignableFrom(t)));

        foreach (var type in typesToRegister)
        {
            services.TryAddTransient(type);

            // If the discovered type is an IPipelineDefinition, register it in the registry
            if (typeof(IPipelineDefinition).IsAssignableFrom(type))
                registry.Register(type);

            // If the discovered type implements IPipelineLineageSinkProvider, also register it against the interface
            // so it can be resolved by the runner via the focused factory interfaces without reflection.
            if (typeof(IPipelineLineageSinkProvider).IsAssignableFrom(type))
                services.TryAddTransient(typeof(IPipelineLineageSinkProvider), type);
        }

        return services;
    }

    /// <summary>
    ///     Registers the core NPipeline services that are required for all configurations.
    /// </summary>
    private static void RegisterCoreServices(IServiceCollection services)
    {
        // Register Core Services (per-run scoped where appropriate)
        services.TryAddTransient<PipelineBuilder>();
        services.TryAddSingleton<IPipelineFactory, PipelineFactory>();
        services.TryAddScoped<INodeFactory, DiContainerNodeFactory>();
        services.TryAddScoped<IPipelineRunner, PipelineRunner>();

        // Register the focused factory interfaces with the same implementation
        services.TryAddScoped<IErrorHandlerFactory, DiHandlerFactory>();
        services.TryAddScoped<ILineageFactory, DiHandlerFactory>();
        services.TryAddScoped<IObservabilityFactory, DiHandlerFactory>();

        // Core execution/observability/persistence services required by the primary PipelineRunner ctor.
        // Without these registrations the container falls back to the parameterless ctor which injects DefaultNodeFactory
        // causing MissingMethodException for nodes with DI-only constructors (ConcurrentQueue<>, etc.).
        services.TryAddScoped<IMergeStrategySelector, MergeStrategySelector>();
        services.TryAddScoped<IPipeMergeService>(sp => new PipeMergeService(sp.GetRequiredService<IMergeStrategySelector>()));
        services.TryAddScoped<ILineage>(_ => NullLineage.Instance);
        services.TryAddScoped<DataStreamWrapperService>();
        services.TryAddScoped<INodeExecutor, NodeExecutor>();
        services.TryAddScoped<IExecutionAnnotationsService, ExecutionAnnotationsService>();
        services.TryAddScoped<ITopologyService, TopologyService>();
        services.TryAddScoped<INodeInstantiationService, NodeInstantiationService>();
        services.TryAddTransient<IErrorHandlingService, ErrorHandlingService>();
        services.TryAddTransient<IPersistenceService, PersistenceService>();
        services.TryAddSingleton<IRuntimePipelineBinder>(_ => RuntimePipelineBinder.Instance);
        // An instance, so the observability extension can tell this default from a surface the app registered.
        services.TryAddSingleton<IObservabilitySurface>(NullObservabilitySurface.Instance);
    }

    /// <summary>
    ///     Creates a pipeline context wired to the services registered in <paramref name="serviceProvider" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Every service <paramref name="configuration" /> leaves unset is taken from the container: the error handler,
    ///         lineage and observability factories, the <see cref="Microsoft.Extensions.Logging.ILoggerFactory" /> and the
    ///         <see cref="Observability.Tracing.IPipelineTracer" />. Every registered <see cref="IExecutionObserver" />, such as
    ///         the metrics observer <c>AddNPipelineObservability</c> registers, is attached to the context. Use this whenever
    ///         you run a pipeline through <see cref="IPipelineRunner" /> yourself rather than through
    ///         <see cref="RunPipelineAsync{TDefinition}(IServiceProvider, CancellationToken)" />; a context created with
    ///         <c>new PipelineContext()</c> gets none of these, so lineage reports, metrics and the framework's logging are
    ///         silently lost.
    ///     </para>
    ///     <para>
    ///         Pass the service provider of the scope the run belongs to, the same one you resolve the runner from, so the
    ///         run gets its own scoped services such as the observability collector. The caller owns the returned context
    ///         and must dispose it.
    ///     </para>
    /// </remarks>
    /// <param name="serviceProvider">The run's (scoped) service provider.</param>
    /// <param name="configuration">The configuration to start from, or null for the default configuration.</param>
    /// <returns>A new pipeline context.</returns>
    /// <example>
    ///     <code>
    ///     await using var scope = serviceProvider.CreateAsyncScope();
    ///     var runner = scope.ServiceProvider.GetRequiredService&lt;IPipelineRunner&gt;();
    ///     await using var context = scope.ServiceProvider.CreatePipelineContext(
    ///         PipelineContextConfiguration.WithCancellation(cancellationToken));
    ///     await runner.RunAsync&lt;MyPipeline&gt;(context);
    ///     </code>
    /// </example>
    public static PipelineContext CreatePipelineContext(this IServiceProvider serviceProvider, PipelineContextConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);

        var context = new PipelineContext((configuration ?? PipelineContextConfiguration.Default).WithServiceDefaults(serviceProvider));

        // Without this the context keeps its NullExecutionObserver and no metrics are collected. It is not part of the
        // configuration, so it is attached to the context itself. Every registered observer is notified.
        if (CompositeExecutionObserver.Combine(serviceProvider.GetServices<IExecutionObserver>(), context.Observability.LoggerFactory) is
            { } executionObserver)
            context.Observability.ExecutionObserver = executionObserver;

        return context;
    }

    /// <summary>
    ///     Runs the specified pipeline definition.
    /// </summary>
    /// <typeparam name="TDefinition">The type of the pipeline definition to run.</typeparam>
    /// <param name="serviceProvider">The service provider to resolve the runner from.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    public static Task RunPipelineAsync<TDefinition>(this IServiceProvider serviceProvider, CancellationToken cancellationToken = default)
        where TDefinition : IPipelineDefinition, new() =>
        serviceProvider.RunPipelineAsync<TDefinition>(null, cancellationToken);

    /// <summary>
    ///     Runs the specified pipeline definition with the given parameters.
    /// </summary>
    /// <typeparam name="TDefinition">The type of the pipeline definition to run.</typeparam>
    /// <param name="serviceProvider">The service provider to resolve the runner from.</param>
    /// <param name="parameters">A dictionary of parameters to pass to the pipeline context.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    public static async Task RunPipelineAsync<TDefinition>(this IServiceProvider serviceProvider, Dictionary<string, object>? parameters,
        CancellationToken cancellationToken = default)
        where TDefinition : IPipelineDefinition, new()
    {
        var scope = serviceProvider.CreateAsyncScope();
        await using var scopeScope = scope.ConfigureAwait(false);
        var sp = scope.ServiceProvider;

        var runner = sp.GetRequiredService<IPipelineRunner>();
        var context = sp.CreatePipelineContext(new PipelineContextConfiguration(parameters, CancellationToken: cancellationToken));

        // The context owns the run-scoped resources handed to it during the run (a dead-letter sink or lineage
        // sink the factory constructed itself, for example). Disposing it releases them even when the run fails.
        await using var contextScope = context.ConfigureAwait(false);

        await runner.RunAsync<TDefinition>(context).ConfigureAwait(false);
    }

    private static IEnumerable<Type> GetLoadableTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types.Where(t => t is not null)!;
        }
    }
}
