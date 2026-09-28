using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Extensions.Lineage.Tests;

/// <summary>
///     With item-level lineage on, items travel between nodes inside lineage packets. A dead-letter sink must still
///     receive the item the pipeline was processing, not the packet around it.
/// </summary>
public sealed class DeadLetterLineageTests
{
    private const string DeadLetterSinkKey = "testing.dead.letter.sink";
    private const string LineageSinkKey = "testing.lineage.sink";

    [Fact]
    public async Task DeadLetteredItem_WithItemLevelLineage_ReachesSinkUnwrapped()
    {
        // Arrange
        var deadLetterSink = new CapturingDeadLetterSink();
        var lineageSink = new CountingLineageSink();
        var runner = new PipelineRunnerBuilder().UseLineage().Build();
        await using var context = new PipelineContext();
        context.Items[DeadLetterSinkKey] = deadLetterSink;
        context.Items[LineageSinkKey] = lineageSink;

        // Act
        await runner.RunAsync<DeadLetteringLineagePipeline>(context);

        // Assert
        lineageSink.Count.Should().BePositive("items must have travelled in lineage packets for the test to mean anything");

        var envelope = deadLetterSink.Captured.Should().ContainSingle().Subject;
        envelope.Item.Should().Be(42);
        envelope.Item.Should().NotBeAssignableTo<ILineageEnvelope>();
        envelope.Attribution.DecisionNodeId.Should().Be("fail");
    }

    private sealed class DeadLetteringLineagePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            _ = builder.EnableItemLevelLineage();
            _ = builder.AddLineageSink((ILineageSink)context.Items[LineageSinkKey]!);
            _ = builder.AddDeadLetterSink((IDeadLetterSink)context.Items[DeadLetterSinkKey]!);

            var source = builder.AddSource<NumbersSourceNode, int>("source");
            var fail = builder.AddTransform<FailingTransform, int, int>("fail");
            var sink = builder.AddSink<DrainingSinkNode, int>("sink");
            _ = builder.Connect(source, fail).Connect(fail, sink);
            _ = builder.AddResiliencePolicy(fail, new DeadLetteringPolicy());
        }
    }

    private sealed class NumbersSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
            => new InMemoryDataStream<int>([1, 42, 3], "numbers");
    }

    private sealed class FailingTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item == 42)
                throw new InvalidOperationException("boom");

            return ValueTask.FromResult(item);
        }
    }

    private sealed class DrainingSinkNode : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }

    private sealed class DeadLetteringPolicy : IResiliencePolicy
    {
        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.DeadLetter);
    }

    private sealed class CapturingDeadLetterSink : IDeadLetterSink
    {
        private readonly List<DeadLetterEnvelope> _captured = [];

        public IReadOnlyList<DeadLetterEnvelope> Captured
        {
            get
            {
                lock (_captured)
                {
                    return [.. _captured];
                }
            }
        }

        public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
        {
            lock (_captured)
            {
                _captured.Add(envelope);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class CountingLineageSink : ILineageSink
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _count);
            return Task.CompletedTask;
        }
    }
}
