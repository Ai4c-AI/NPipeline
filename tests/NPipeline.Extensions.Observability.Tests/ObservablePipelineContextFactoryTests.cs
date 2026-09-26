using System.Collections.Concurrent;
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Observability;
using NPipeline.Lineage;
using NPipeline.Lineage.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Observability.Tests;

/// <summary>
///     Tests for the <see cref="ObservablePipelineContextFactory" /> and related DI registrations.
/// </summary>
public sealed class ObservablePipelineContextFactoryTests
{
    private static readonly Guid s_pipelineId = Guid.Parse("11111111-1111-1111-1111-111111111111");

    [Fact]
    public async Task Create_ReturnsContextWithExecutionObserverSet()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();
        var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var factory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();

        // Act
        await using var context = factory.Create();

        // Assert
        Assert.NotNull(context);
        Assert.NotNull(context.Observability.ExecutionObserver);
        Assert.IsType<MetricsCollectingExecutionObserver>(context.Observability.ExecutionObserver);
    }

    [Fact]
    public async Task Create_WithCancellationToken_ReturnsContextWithCancellationToken()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();
        var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var factory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();
        using var cts = new CancellationTokenSource();

        // Act
        await using var context = factory.Create(cts.Token);

        // Assert
        Assert.Equal(cts.Token, context.CancellationToken);
        Assert.IsType<MetricsCollectingExecutionObserver>(context.Observability.ExecutionObserver);
    }

    [Fact]
    public void IExecutionObserver_IsRegisteredWithCollector()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();
        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        // Act
        var observer = scope.ServiceProvider.GetRequiredService<IExecutionObserver>();
        var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

        // Assert
        Assert.IsType<MetricsCollectingExecutionObserver>(observer);

        // Verify the observer is connected to the collector by recording an event
        var startEvent = new NodeExecutionStarted("test-node", "TestNode", DateTimeOffset.UtcNow, s_pipelineId);
        observer.OnNodeStarted(startEvent);

        var metrics = collector.GetNodeMetrics("test-node", s_pipelineId);
        Assert.NotNull(metrics);
        Assert.Equal("test-node", metrics.NodeId);
    }

    [Fact]
    public void ScopedRegistrations_CreateNewInstancesPerScope()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();
        var provider = services.BuildServiceProvider();

        IObservabilityCollector collector1;
        IObservabilityCollector collector2;

        // Act
        using (var scope1 = provider.CreateScope())
        {
            collector1 = scope1.ServiceProvider.GetRequiredService<IObservabilityCollector>();
        }

        using (var scope2 = provider.CreateScope())
        {
            collector2 = scope2.ServiceProvider.GetRequiredService<IObservabilityCollector>();
        }

        // Assert
        Assert.NotSame(collector1, collector2);
    }

    [Fact]
    public async Task Create_WithLineageRegistered_ProducesPipelineLineageReport()
    {
        // Arrange - the pattern the Observability sample uses, plus lineage. The factory used to leave the lineage
        // factory unset, so the context fell back to one that cannot produce reports and none was ever written.
        var sink = new CollectingPipelineLineageSink();
        var services = new ServiceCollection();
        _ = services.AddNPipeline(typeof(ObservablePipelineContextFactoryTests).Assembly);
        _ = services.AddNPipelineObservability();
        _ = services.AddNPipelineLineage(_ => sink);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();
        await using var context = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>().Create();

        // Act
        await runner.RunAsync<LineageReportPipeline>(context);

        // Assert
        var report = Assert.Single(sink.Reports);
        Assert.Equal(nameof(LineageReportPipeline), report.Pipeline);
    }

    [Fact]
    public async Task Create_TakesUnsetServicesFromTheScope()
    {
        // Arrange
        var loggerFactory = A.Fake<ILoggerFactory>();
        var tracer = A.Fake<IPipelineTracer>();
        var lineageFactory = A.Fake<ILineageFactory>();
        var errorHandlerFactory = A.Fake<IErrorHandlerFactory>();

        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();
        _ = services.AddSingleton(loggerFactory);
        _ = services.AddSingleton(tracer);
        _ = services.AddSingleton(lineageFactory);
        _ = services.AddSingleton(errorHandlerFactory);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var factory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();

        // Act
        await using var context = factory.Create();

        // Assert
        Assert.Same(loggerFactory, context.Observability.LoggerFactory);
        Assert.Same(tracer, context.Observability.Tracer);
        Assert.Same(lineageFactory, context.Lineage.LineageFactory);
        Assert.Same(errorHandlerFactory, context.ErrorHandlerFactory);
        Assert.Same(scope.ServiceProvider.GetRequiredService<IObservabilityFactory>(), context.Observability.ObservabilityFactory);
    }

    [Fact]
    public async Task Create_KeepsServicesTheCallerSupplies()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();
        _ = services.AddSingleton(A.Fake<ILoggerFactory>());
        _ = services.AddSingleton(A.Fake<ILineageFactory>());
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var factory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();

        var ownLoggerFactory = A.Fake<ILoggerFactory>();
        var ownLineageFactory = A.Fake<ILineageFactory>();
        var ownObservabilityFactory = A.Fake<IObservabilityFactory>();

        var configuration = new PipelineContextConfiguration(
            LoggerFactory: ownLoggerFactory,
            LineageFactory: ownLineageFactory,
            ObservabilityFactory: ownObservabilityFactory);

        // Act
        await using var context = factory.Create(configuration);

        // Assert - the observability factory used to be overwritten even when the caller supplied one.
        Assert.Same(ownLoggerFactory, context.Observability.LoggerFactory);
        Assert.Same(ownLineageFactory, context.Lineage.LineageFactory);
        Assert.Same(ownObservabilityFactory, context.Observability.ObservabilityFactory);
        Assert.IsType<MetricsCollectingExecutionObserver>(context.Observability.ExecutionObserver);
    }

    [Fact]
    public async Task CreatePipelineContext_ProducesMetricsAndALineageReport()
    {
        // Arrange - the DI package's CreatePipelineContext, for callers that run pipelines through the runner themselves.
        var sink = new CollectingPipelineLineageSink();
        var services = new ServiceCollection();
        _ = services.AddNPipeline(typeof(ObservablePipelineContextFactoryTests).Assembly);
        _ = services.AddNPipelineObservability();
        _ = services.AddNPipelineLineage(_ => sink);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();
        await using var context = scope.ServiceProvider.CreatePipelineContext();

        // Act
        await runner.RunAsync<LineageReportPipeline>(context);

        // Assert
        _ = Assert.Single(sink.Reports);
        var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();
        Assert.NotNull(collector.GetNodeMetrics("source", context.RunIdentity.PipelineId));
        Assert.NotNull(collector.GetNodeMetrics("sink", context.RunIdentity.PipelineId));
    }

    private sealed class LineageReportPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ThreeItemSource, int>("source");
            var sink = builder.AddSink<DiscardingSink, int>("sink");
            _ = builder.Connect(source, sink);
        }
    }

    public sealed class ThreeItemSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new InMemoryDataStream<int>([1, 2, 3]);
    }

    public sealed class DiscardingSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }

    private sealed class CollectingPipelineLineageSink : IPipelineLineageSink
    {
        public ConcurrentQueue<PipelineLineageReport> Reports { get; } = new();

        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
        {
            Reports.Enqueue(report);
            return Task.CompletedTask;
        }
    }
}
