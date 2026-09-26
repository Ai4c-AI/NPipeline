using Microsoft.Extensions.DependencyInjection;
using NPipeline.Attributes.Nodes;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.DataFlow.Windowing;
using NPipeline.Execution;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Lineage.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Observability.Metrics;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Item counts for the node kinds that run no execution strategy: sources, joins, aggregates and sinks. They used to
///     report zero items processed and emitted, even with observability enabled on the node.
/// </summary>
public sealed class NodeItemCountTests
{
    private static readonly DateTimeOffset s_eventTime = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Join_CountsEveryInputItemAsProcessedAndEachMatchAsEmitted()
    {
        // Act - three left items (keys 1-3), two right items (keys 2-3), inner join: two matches.
        var collector = await RunAsync<JoinPipeline>();

        // Assert
        AssertCounts(collector, "left", 0, 3);
        AssertCounts(collector, "right", 0, 2);
        AssertCounts(collector, "join", 5, 2);
        AssertCounts(collector, "sink", 2, 0);
    }

    [Fact]
    public async Task Aggregate_CountsEveryInputItemAsProcessedAndEachResultAsEmitted()
    {
        // Act - ten items over two keys, all in one window: two results.
        var collector = await RunAsync<AggregatePipeline>();

        // Assert
        AssertCounts(collector, "source", 0, 10);
        AssertCounts(collector, "aggregate", 10, 2);
        AssertCounts(collector, "sink", 2, 0);
    }

    [Fact]
    public async Task BranchingSource_CountsEachItemOnceNotOncePerConsumer()
    {
        // Act
        var collector = await RunAsync<BranchingPipeline>();

        // Assert
        AssertCounts(collector, "source", 0, 3);
        AssertCounts(collector, "sinkA", 3, 0);
        AssertCounts(collector, "sinkB", 3, 0);
    }

    [Fact]
    public async Task ItemLevelLineage_DoesNotChangeTheCounts()
    {
        // Act
        var collector = await RunAsync<LineagePipeline>(withLineage: true);

        // Assert
        AssertCounts(collector, "source", 0, 3);
        AssertCounts(collector, "transform", 3, 3);
        AssertCounts(collector, "sink", 3, 0);
    }

    [Fact]
    public async Task UnobservedNodes_RecordNoItemCounts()
    {
        // Act - only the transform is observed.
        var collector = await RunAsync<PartiallyObservedPipeline>();

        // Assert
        AssertCounts(collector, "transform", 3, 3);
        Assert.Equal(0, TestHelpers.GetNodeMetricsById(collector, "source")?.ItemsEmitted ?? 0);
        Assert.Equal(0, TestHelpers.GetNodeMetricsById(collector, "sink")?.ItemsProcessed ?? 0);
    }

    [Fact]
    public async Task PipelineMetrics_SumItemsProcessedAcrossNodes()
    {
        // Act
        var collector = await RunAsync<LineagePipeline>(withLineage: true);
        var metrics = collector.CreatePipelineMetrics("p", collector.GetNodeMetrics()[0].PipelineId, Guid.NewGuid(), s_eventTime, s_eventTime, true);

        // Assert - documented as the sum across all nodes: the transform's 3 plus the sink's 3.
        Assert.Equal(6, metrics.TotalItemsProcessed);
    }

    private static void AssertCounts(IObservabilityCollector collector, string nodeId, long processed, long emitted)
    {
        var metrics = TestHelpers.GetNodeMetricsById(collector, nodeId);
        Assert.NotNull(metrics);
        Assert.True(metrics.Success, $"{nodeId} should succeed");
        Assert.Equal(processed, metrics.ItemsProcessed);
        Assert.Equal(emitted, metrics.ItemsEmitted);
    }

    private static async Task<IObservabilityCollector> RunAsync<TPipeline>(bool withLineage = false)
        where TPipeline : IPipelineDefinition, new()
    {
        var services = new ServiceCollection();
        _ = services.AddNPipeline(typeof(NodeItemCountTests).Assembly);
        _ = services.AddNPipelineObservability();

        if (withLineage)
            _ = services.AddNPipelineLineage();

        var provider = services.BuildServiceProvider();
        var scope = provider.CreateAsyncScope();
        var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();
        await using var context = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>().Create();

        await runner.RunAsync<TPipeline>(context);

        return scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();
    }

    public sealed record LeftItem(int Key);

    public sealed record RightItem(int Key);

    public sealed record Reading(int Key);

    private sealed class JoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var left = builder.AddSource<LeftSource, LeftItem>("left").WithObservability(builder);
            var right = builder.AddSource<RightSource, RightItem>("right").WithObservability(builder);
            var join = builder.AddJoin<KeyJoin, LeftItem, RightItem, int>("join").WithObservability(builder);
            var sink = builder.AddSink<DiscardingSink<int>, int>("sink").WithObservability(builder);

            _ = builder.Connect(left, join);
            _ = builder.Connect(right, join);
            _ = builder.Connect(join, sink);
        }
    }

    private sealed class AggregatePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ReadingSource, Reading>("source").WithObservability(builder);
            var aggregate = builder.AddAggregate<CountByKey, Reading, int, int>("aggregate").WithObservability(builder);
            var sink = builder.AddSink<DiscardingSink<int>, int>("sink").WithObservability(builder);

            _ = builder.Connect(source, aggregate);
            _ = builder.Connect(aggregate, sink);
        }
    }

    private sealed class BranchingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ThreeIntSource, int>("source").WithObservability(builder);
            var sinkA = builder.AddSink<DiscardingSink<int>, int>("sinkA").WithObservability(builder);
            var sinkB = builder.AddSink<DiscardingSink<int>, int>("sinkB").WithObservability(builder);

            _ = builder.Connect(source, sinkA);
            _ = builder.Connect(source, sinkB);
        }
    }

    private sealed class LineagePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            builder.EnableItemLevelLineage();

            var source = builder.AddSource<ThreeIntSource, int>("source").WithObservability(builder);
            var transform = builder.AddTransform<Doubler, int, int>("transform").WithObservability(builder);
            var sink = builder.AddSink<DiscardingSink<int>, int>("sink").WithObservability(builder);

            _ = builder.Connect(source, transform);
            _ = builder.Connect(transform, sink);
        }
    }

    private sealed class PartiallyObservedPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ThreeIntSource, int>("source");
            var transform = builder.AddTransform<Doubler, int, int>("transform").WithObservability(builder);
            var sink = builder.AddSink<DiscardingSink<int>, int>("sink");

            _ = builder.Connect(source, transform);
            _ = builder.Connect(transform, sink);
        }
    }

    public sealed class LeftSource : SourceNode<LeftItem>
    {
        public override IDataStream<LeftItem> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new InMemoryDataStream<LeftItem>([new LeftItem(1), new LeftItem(2), new LeftItem(3)]);
    }

    public sealed class RightSource : SourceNode<RightItem>
    {
        public override IDataStream<RightItem> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new InMemoryDataStream<RightItem>([new RightItem(2), new RightItem(3)]);
    }

    public sealed class ReadingSource : SourceNode<Reading>
    {
        public override IDataStream<Reading> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new InMemoryDataStream<Reading>(Enumerable.Range(0, 10).Select(i => new Reading(i % 2)).ToList());
    }

    public sealed class ThreeIntSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new InMemoryDataStream<int>([1, 2, 3]);
    }

    [KeySelector(typeof(LeftItem), nameof(LeftItem.Key))]
    [KeySelector(typeof(RightItem), nameof(RightItem.Key))]
    public sealed class KeyJoin : KeyedJoinNode<int, LeftItem, RightItem, int>
    {
        public override int CreateOutput(LeftItem item1, RightItem item2) => item1.Key;
    }

    public sealed class CountByKey() : AggregateNode<Reading, int, int>(
        new AggregateNodeConfiguration<Reading>(WindowAssigner.Tumbling(TimeSpan.FromMinutes(1)), _ => s_eventTime, TimeSpan.Zero))
    {
        public override int GetKey(Reading item) => item.Key;

        public override int CreateAccumulator() => 0;

        public override int Accumulate(int accumulator, Reading item) => accumulator + 1;
    }

    public sealed class Doubler : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) => ValueTask.FromResult(item * 2);
    }

    public sealed class DiscardingSink<T> : SinkNode<T>
    {
        public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }
}
