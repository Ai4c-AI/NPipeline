using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Observability;

/// <summary>
///     Factory for creating pipeline contexts with observability pre-configured.
/// </summary>
/// <remarks>
///     This factory creates <see cref="PipelineContext" /> instances that are automatically
///     wired up with <see cref="MetricsCollectingExecutionObserver" /> to enable automatic
///     metrics collection during pipeline execution.
/// </remarks>
public sealed class ObservablePipelineContextFactory : IObservablePipelineContextFactory
{
    private readonly IExecutionObserver _executionObserver;
    private readonly IServiceProvider _serviceProvider;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ObservablePipelineContextFactory" /> class.
    /// </summary>
    /// <param name="serviceProvider">The service provider for resolving dependencies.</param>
    /// <param name="executionObserver">The execution observer to use for the context.</param>
    public ObservablePipelineContextFactory(IServiceProvider serviceProvider, IExecutionObserver executionObserver)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _executionObserver = executionObserver ?? throw new ArgumentNullException(nameof(executionObserver));
    }

    /// <inheritdoc />
    public PipelineContext Create(CancellationToken cancellationToken = default)
    {
        var config = cancellationToken == default
            ? PipelineContextConfiguration.Default
            : PipelineContextConfiguration.WithCancellation(cancellationToken);

        return Create(config);
    }

    /// <inheritdoc />
    public PipelineContext Create(PipelineContextConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        // Fill whatever the caller left unset from the current scope: the error handler, lineage and observability
        // factories, the logger factory and the tracer (for example the OpenTelemetry tracer). Without the lineage
        // factory no pipeline lineage report is produced, and without the logger factory the framework's logging is
        // silent. Resolving from the scope gives the run its own scoped IObservabilityCollector.
        var configWithServices = configuration.WithServiceDefaults(_serviceProvider);

        if (configWithServices.ObservabilityFactory is null)
            configWithServices = configWithServices with { ObservabilityFactory = new DiObservabilityFactory(_serviceProvider) };

        var context = new PipelineContext(configWithServices);
        context.Observability.ExecutionObserver = _executionObserver;

        return context;
    }
}
