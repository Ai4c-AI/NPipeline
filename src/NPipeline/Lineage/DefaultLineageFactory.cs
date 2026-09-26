using System.Reflection;
using Microsoft.Extensions.Logging;
using NPipeline.Graph;
using NPipeline.Observability.Logging;

namespace NPipeline.Lineage;

/// <summary>
///     Default implementation of <see cref="ILineageFactory" /> that creates lineage-related components
///     using reflection with proper error handling and logging.
/// </summary>
internal sealed class DefaultLineageFactory : ILineageFactory
{
    private readonly ILogger _logger;
    private readonly ILoggerFactory _loggerFactory;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DefaultLineageFactory" /> class.
    /// </summary>
    /// <param name="loggerFactory">Optional logger factory for diagnostic logging. Defaults to a no-op logger if not provided.</param>
    public DefaultLineageFactory(ILoggerFactory? loggerFactory = null)
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger(nameof(DefaultLineageFactory));
    }

    /// <summary>
    ///     Creates an instance of the specified lineage sink type.
    /// </summary>
    /// <param name="sinkType">The type of the lineage sink to create.</param>
    /// <returns>An instance of <see cref="ILineageSink" />, or null if it cannot be created.</returns>
    public ILineageSink? CreateLineageSink(Type sinkType) => TryCreateInstance<ILineageSink>(sinkType);

    /// <summary>
    ///     Creates an instance of the specified pipeline lineage sink type (explicit configuration path).
    /// </summary>
    /// <remarks>
    ///     Use this when a concrete sink type is explicitly configured via the builder or context. This is an imperative,
    ///     unambiguous request to construct that sink (typically via DI and falling back to ActivatorUtilities).
    /// </remarks>
    /// <param name="sinkType">The type of the pipeline lineage sink to create.</param>
    /// <returns>An instance of <see cref="IPipelineLineageSink" />, or null if it cannot be created.</returns>
    public IPipelineLineageSink? CreatePipelineLineageSink(Type sinkType) => TryCreateInstance<IPipelineLineageSink>(sinkType);

    /// <summary>
    ///     Resolves an optional provider capable of supplying a default pipeline lineage sink (implicit default path).
    /// </summary>
    /// <remarks>
    ///     This is consulted only when no explicit sink (instance or type) is configured and item-level lineage is enabled.
    ///     It allows optional packages (e.g., NPipeline.Extensions.Lineage) to supply a sensible default without reflection.
    ///     Returns null when no provider is registered or available.
    /// </remarks>
    /// <returns>An <see cref="IPipelineLineageSinkProvider" /> instance or null.</returns>
    public IPipelineLineageSinkProvider? ResolvePipelineLineageSinkProvider() =>

        // No DI container available in the default factory; cannot supply a provider.
        // This is expected behavior - lineage sink providers require dependency injection.
        null;

    /// <summary>
    ///     Resolves an optional lineage collector for tracking data lineage.
    /// </summary>
    /// <returns>An <see cref="ILineageCollector" /> instance or null if lineage is not enabled.</returns>
    public ILineageCollector? ResolveLineageCollector() =>

        // No DI container available in the default factory; cannot supply a collector.
        // Lineage collection requires DI registration through services.AddLineageTracking().
        null;

    /// <summary>
    ///     Creates a lineage report for a pipeline run.
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="pipelineId">The pipeline identifier.</param>
    /// <param name="graph">The pipeline graph.</param>
    /// <param name="runId">The run identifier.</param>
    /// <returns>A <see cref="PipelineLineageReport" />, or null if lineage reporting is not available.</returns>
    public PipelineLineageReport? CreateLineageReport(string pipelineName, Guid pipelineId, PipelineGraph graph, Guid runId)
        => null;

    /// <summary>
    ///     Attempts to create an instance of the specified type using reflection.
    /// </summary>
    /// <remarks>
    ///     Uses the public constructor with the most parameters that can all be supplied: loggers (<see cref="ILoggerFactory" />,
    ///     <see cref="ILogger" /> or <see cref="ILogger{TCategoryName}" />) come from this factory's logger factory, and any other
    ///     parameter must be optional. A sink such as <c>LoggingPipelineLineageSink</c>, which takes an optional logger, therefore
    ///     logs through the run's logging rather than a null logger.
    /// </remarks>
    /// <typeparam name="T">The interface type expected.</typeparam>
    /// <param name="type">The concrete type to instantiate.</param>
    /// <returns>The created instance or null if creation fails.</returns>
    private T? TryCreateInstance<T>(Type type) where T : class
    {
        if (!typeof(T).IsAssignableFrom(type))
        {
            DefaultLineageFactoryLogMessages.LineageSinkCreationFailed(_logger, type?.FullName ?? "null");
            return null;
        }

        try
        {
            if (CreateInstance(type) is T instance)
                return instance;
        }
        catch
        {
            // Fall through to log failure
        }

        DefaultLineageFactoryLogMessages.LineageSinkCreationFailed(_logger, type?.FullName ?? "null");

        return null;
    }

    private object? CreateInstance(Type type)
    {
        ConstructorInfo? selected = null;
        object?[] selectedArguments = [];

        foreach (var constructor in type.GetConstructors())
        {
            var parameters = constructor.GetParameters();

            if (selected is not null && parameters.Length <= selectedArguments.Length)
                continue;

            var arguments = new object?[parameters.Length];
            var satisfiable = true;

            for (var i = 0; i < parameters.Length && satisfiable; i++)
            {
                satisfiable = TryResolveArgument(parameters[i], out arguments[i]);
            }

            if (satisfiable)
            {
                selected = constructor;
                selectedArguments = arguments;
            }
        }

        return selected?.Invoke(selectedArguments);
    }

    private bool TryResolveArgument(ParameterInfo parameter, out object? value)
    {
        var parameterType = parameter.ParameterType;

        if (parameterType == typeof(ILoggerFactory))
        {
            value = _loggerFactory;
            return true;
        }

        if (parameterType == typeof(ILogger))
        {
            value = _loggerFactory.CreateLogger(parameter.Member.DeclaringType?.FullName ?? nameof(DefaultLineageFactory));
            return true;
        }

        if (parameterType.IsGenericType && parameterType.GetGenericTypeDefinition() == typeof(ILogger<>))
        {
            value = Activator.CreateInstance(typeof(Logger<>).MakeGenericType(parameterType.GetGenericArguments()), _loggerFactory);
            return true;
        }

        if (parameter.HasDefaultValue)
        {
            value = parameter.DefaultValue;
            return true;
        }

        value = null;
        return false;
    }
}
