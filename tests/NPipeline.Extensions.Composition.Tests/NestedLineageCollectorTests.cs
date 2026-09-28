using System.Collections.Concurrent;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Extensions.Composition.Tests;

/// <summary>
///     A sub-pipeline inherits its parent's lineage sink, which already tees every record into the run's
///     <see cref="ILineageCollector" />. The collector must still receive each record once.
/// </summary>
public sealed class NestedLineageCollectorTests
{
    private const string SinkKey = "testing.lineage.sink";

    [Fact]
    public async Task SubPipelineRecords_ReachTheCollectorOnce()
    {
        // Arrange
        var collector = new CountingCollector();
        var sink = new CountingLineageSink();
        var runner = new PipelineRunnerBuilder().UseLineage().Build();

        await using var context = new PipelineContext(
            PipelineContextConfiguration.Default with { LineageFactory = new CollectorLineageFactory(collector) });

        context.Items[SinkKey] = sink;

        // Act
        await runner.RunAsync<ParentPipeline>(context);

        // Assert - the child's records reach the parent's sink, and the collector sees exactly what the sink sees.
        sink.Records.Should().Contain(r => r.NodeId.EndsWith("child-transform", StringComparison.Ordinal));
        collector.Records.Should().HaveCount(sink.Records.Count);
    }

    [Fact]
    public async Task SubPipelineRecords_ThroughADecoratedInheritedSink_ReachTheCollectorOnce()
    {
        // Arrange - every sub-pipeline wraps the sink it inherits, as instrumentation such as NPipeline Studio does.
        var collector = new CountingCollector();
        var sink = new CountingLineageSink();
        var runner = new PipelineRunnerBuilder().UseLineage().Build();

        await using var context = new PipelineContext(
            PipelineContextConfiguration.Default with { LineageFactory = new CollectorLineageFactory(collector) });

        context.Items[SinkKey] = sink;

        context.Properties[PipelineContextKeys.SubPipelineContextInitializer] = new Action<PipelineContext, PipelineContext>((_, child) =>
            child.Properties[PipelineContextKeys.LineageSinkDecorator] =
                new Func<ILineageSink?, ILineageSink?>(inner => inner is null ? null : new ForwardingLineageSink(inner)));

        // Act
        await runner.RunAsync<ParentPipeline>(context);

        // Assert
        sink.Records.Should().Contain(r => r.NodeId.EndsWith("child-transform", StringComparison.Ordinal));
        collector.Records.Should().HaveCount(sink.Records.Count);
    }

    [Fact]
    public async Task SubPipelineWithItsOwnSink_StillTeesIntoTheCollector()
    {
        // Arrange - the child's records go to its own sink, never through the parent's tee.
        var collector = new CountingCollector();
        var parentSink = new CountingLineageSink();
        var runner = new PipelineRunnerBuilder().UseLineage().Build();

        await using var context = new PipelineContext(
            PipelineContextConfiguration.Default with { LineageFactory = new CollectorLineageFactory(collector) });

        context.Items[SinkKey] = parentSink;
        ChildWithOwnSinkPipeline.Sink = new CountingLineageSink();

        // Act
        await runner.RunAsync<ParentWithOwnSinkChildPipeline>(context);

        // Assert
        var childSink = ChildWithOwnSinkPipeline.Sink;
        childSink.Records.Should().NotBeEmpty();
        collector.Records.Should().HaveCount(parentSink.Records.Count + childSink.Records.Count);
    }

    private sealed class ParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            _ = builder.EnableItemLevelLineage();
            _ = builder.AddLineageSink((ILineageSink)context.Items[SinkKey]!);

            var source = builder.AddSource<NumbersSource, int>("source");
            var composite = builder.AddComposite<int, int, ChildPipeline>("composite", CompositeContextConfiguration.Default);
            var sinkNode = builder.AddSink<DrainingSink, int>("sink");

            _ = builder.Connect(source, composite);
            _ = builder.Connect(composite, sinkNode);
        }
    }

    private sealed class ChildPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            _ = builder.EnableItemLevelLineage();

            var input = builder.AddSource<PipelineInputSource<int>, int>("child-input");
            var transform = builder.AddTransform<PassThrough, int, int>("child-transform");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("child-output");

            _ = builder.Connect(input, transform);
            _ = builder.Connect(transform, output);
        }
    }

    private sealed class ParentWithOwnSinkChildPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            _ = builder.EnableItemLevelLineage();
            _ = builder.AddLineageSink((ILineageSink)context.Items[SinkKey]!);

            var source = builder.AddSource<NumbersSource, int>("source");
            var composite = builder.AddComposite<int, int, ChildWithOwnSinkPipeline>("composite", CompositeContextConfiguration.Default);
            var sinkNode = builder.AddSink<DrainingSink, int>("sink");

            _ = builder.Connect(source, composite);
            _ = builder.Connect(composite, sinkNode);
        }
    }

    private sealed class ChildWithOwnSinkPipeline : IPipelineDefinition
    {
        public static CountingLineageSink Sink { get; set; } = new();

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            _ = builder.EnableItemLevelLineage();
            _ = builder.AddLineageSink(Sink);

            var input = builder.AddSource<PipelineInputSource<int>, int>("child-input");
            var transform = builder.AddTransform<PassThrough, int, int>("child-transform");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("child-output");

            _ = builder.Connect(input, transform);
            _ = builder.Connect(transform, output);
        }
    }

    private sealed class ForwardingLineageSink(ILineageSink inner) : ILineageSink
    {
        public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken) => inner.RecordAsync(record, cancellationToken);
    }

    private sealed class NumbersSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new InMemoryDataStream<int>([1, 2, 3], "numbers");
    }

    private sealed class PassThrough : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(item);
    }

    private sealed class DrainingSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }

    private sealed class CountingLineageSink : ILineageSink
    {
        private readonly ConcurrentQueue<LineageRecord> _records = new();

        public IReadOnlyCollection<LineageRecord> Records => [.. _records];

        public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken)
        {
            _records.Enqueue(record);
            return Task.CompletedTask;
        }
    }

    private sealed class CountingCollector : ILineageCollector
    {
        private readonly LineageCollector _inner = new();
        private readonly ConcurrentQueue<LineageRecord> _records = new();

        public IReadOnlyCollection<LineageRecord> Records => [.. _records];

        public LineagePacket<T> CreateLineagePacket<T>(T item, string sourceNodeId) => _inner.CreateLineagePacket(item, sourceNodeId);

        public void Record(LineageRecord record)
        {
            _records.Enqueue(record);
            _inner.Record(record);
        }

        public bool ShouldCollectLineage(Guid correlationId, LineageOptions? options) => _inner.ShouldCollectLineage(correlationId, options);

        public IReadOnlyList<LineageRecord> GetCorrelationHistory(Guid correlationId) => _inner.GetCorrelationHistory(correlationId);

        public LineageOutcomeReason? GetTerminalReason(Guid correlationId) => _inner.GetTerminalReason(correlationId);

        public IReadOnlyList<LineageRecord> GetAllRecords() => _inner.GetAllRecords();

        public IReadOnlyList<Guid> GetUnresolvedCorrelations() => _inner.GetUnresolvedCorrelations();

        public void Clear() => _inner.Clear();
    }

    private sealed class CollectorLineageFactory(ILineageCollector collector) : ILineageFactory
    {
        public ILineageSink? CreateLineageSink(Type sinkType) => null;

        public IPipelineLineageSink? CreatePipelineLineageSink(Type sinkType) => null;

        public IPipelineLineageSinkProvider? ResolvePipelineLineageSinkProvider() => null;

        public ILineageCollector? ResolveLineageCollector() => collector;

        public PipelineLineageReport? CreateLineageReport(string pipelineName, Guid pipelineId, PipelineGraph graph, Guid runId) => null;
    }
}
