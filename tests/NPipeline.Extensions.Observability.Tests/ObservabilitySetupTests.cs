using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Configuration;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Covers the ways a user could end up with no metrics or zero item counts without being told why: nodes without
///     <c>WithObservability</c>, a context created outside the container, and a runner built without DI.
/// </summary>
public sealed class ObservabilitySetupTests
{
    [Fact]
    public async Task NodesWithoutOptions_ReportItemCountsAsNotRecorded()
    {
        // Act - only the transform is observed.
        var collector = await RunAsync<PartiallyObservedPipeline>(ObservabilityExtensionOptions.Default);

        // Assert
        Assert.False(TestHelpers.GetNodeMetricsById(collector, "source")!.ItemCountsRecorded);
        Assert.False(TestHelpers.GetNodeMetricsById(collector, "sink")!.ItemCountsRecorded);
        Assert.True(TestHelpers.GetNodeMetricsById(collector, "transform")!.ItemCountsRecorded);
    }

    [Fact]
    public async Task AutoObserveAllNodes_CountsNodesWithoutOptions()
    {
        // Act
        var collector = await RunAsync<PartiallyObservedPipeline>(new ObservabilityExtensionOptions { AutoObserveAllNodes = true });

        // Assert
        var source = TestHelpers.GetNodeMetricsById(collector, "source")!;
        var transform = TestHelpers.GetNodeMetricsById(collector, "transform")!;
        var sink = TestHelpers.GetNodeMetricsById(collector, "sink")!;

        Assert.True(source.ItemCountsRecorded);
        Assert.Equal(3, source.ItemsEmitted);
        Assert.Equal(3, transform.ItemsProcessed);
        Assert.Equal(3, transform.ItemsEmitted);
        Assert.True(sink.ItemCountsRecorded);
        Assert.Equal(3, sink.ItemsProcessed);

        var metrics = collector.CreatePipelineMetrics("p", source.PipelineId, Guid.NewGuid(), DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, true);
        Assert.Equal(3, metrics.ItemsIn);
        Assert.Equal(3, metrics.ItemsOut);
    }

    [Fact]
    public async Task AutoObserveAllNodes_NodesOwnOptionsWin()
    {
        // Act - the sink opts out of item counts explicitly.
        var collector = await RunAsync<SinkOptsOutPipeline>(new ObservabilityExtensionOptions { AutoObserveAllNodes = true });

        // Assert
        Assert.True(TestHelpers.GetNodeMetricsById(collector, "source")!.ItemCountsRecorded);
        Assert.False(TestHelpers.GetNodeMetricsById(collector, "sink")!.ItemCountsRecorded);
    }

    [Fact]
    public async Task ContextCreatedOutsideTheContainer_WarnsThatNoMetricsWillBeRecorded()
    {
        // Arrange
        var logs = new CapturingLoggerProvider();
        await using var provider = BuildProvider(logs);
        await using var scope = provider.CreateAsyncScope();
        var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(logs));
        await using var context = new PipelineContext(PipelineContextConfiguration.WithLogging(loggerFactory));

        // Act
        await runner.RunAsync<PartiallyObservedPipeline>(context);

        // Assert
        Assert.Contains(logs.Warnings, w => w.Contains("no observability collector", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ContextFromTheContainer_DoesNotWarn()
    {
        // Arrange
        var logs = new CapturingLoggerProvider();
        await using var provider = BuildProvider(logs);

        // Act
        await provider.RunPipelineAsync<PartiallyObservedPipeline>();

        // Assert
        Assert.Empty(logs.Warnings);
    }

    [Fact]
    public async Task RunnerWithoutDi_WithObservedNodes_WarnsThatNoMetricsWillBeRecorded()
    {
        // Arrange
        var logs = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(logs));
        var runner = PipelineRunner.Create();
        await using var context = new PipelineContext(PipelineContextConfiguration.WithLogging(loggerFactory));

        // Act
        await runner.RunAsync<PartiallyObservedPipeline>(context);

        // Assert
        var warning = Assert.Single(logs.Warnings);
        Assert.Contains("WithObservability", warning, StringComparison.Ordinal);
        Assert.Contains("AddNPipelineObservability", warning, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunnerWithoutDi_WithoutObservedNodes_DoesNotWarn()
    {
        // Arrange
        var logs = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(logs));
        var runner = PipelineRunner.Create();
        await using var context = new PipelineContext(PipelineContextConfiguration.WithLogging(loggerFactory));

        // Act
        await runner.RunAsync<UnobservedPipeline>(context);

        // Assert
        Assert.Empty(logs.Warnings);
    }

    private static ServiceProvider BuildProvider(CapturingLoggerProvider logs)
    {
        var services = new ServiceCollection();
        _ = services.AddLogging(b => b.AddProvider(logs));
        _ = services.AddNPipeline(typeof(ObservabilitySetupTests).Assembly);
        _ = services.AddNPipelineObservability();
        return services.BuildServiceProvider();
    }

    private static async Task<IObservabilityCollector> RunAsync<TPipeline>(ObservabilityExtensionOptions options)
        where TPipeline : IPipelineDefinition, new()
    {
        var services = new ServiceCollection();
        _ = services.AddNPipeline(typeof(ObservabilitySetupTests).Assembly);
        _ = services.AddNPipelineObservability(options);

        var provider = services.BuildServiceProvider();
        var scope = provider.CreateAsyncScope();
        var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();
        await using var context = scope.ServiceProvider.CreatePipelineContext();

        await runner.RunAsync<TPipeline>(context);

        return scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();
    }

    private sealed class PartiallyObservedPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ThreeIntSource, int>("source");
            var transform = builder.AddTransform<Doubler, int, int>("transform").WithObservability(builder);
            var sink = builder.AddSink<DiscardingSink, int>("sink");

            _ = builder.Connect(source, transform);
            _ = builder.Connect(transform, sink);
        }
    }

    private sealed class SinkOptsOutPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ThreeIntSource, int>("source");
            var sink = builder.AddSink<DiscardingSink, int>("sink")
                .WithObservability(builder, ObservabilityOptions.Default with { RecordItemCounts = false });

            _ = builder.Connect(source, sink);
        }
    }

    private sealed class UnobservedPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ThreeIntSource, int>("source");
            var sink = builder.AddSink<DiscardingSink, int>("sink");

            _ = builder.Connect(source, sink);
        }
    }

    public sealed class ThreeIntSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new InMemoryDataStream<int>([1, 2, 3]);
    }

    public sealed class Doubler : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(item * 2);
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

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<string> _warnings = new();

        public IReadOnlyCollection<string> Warnings => _warnings;

        public ILogger CreateLogger(string categoryName) => new WarningLogger(_warnings);

        public void Dispose()
        {
        }

        private sealed class WarningLogger(ConcurrentQueue<string> warnings) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                if (logLevel >= LogLevel.Warning)
                    warnings.Enqueue(formatter(state, exception));
            }
        }
    }
}
