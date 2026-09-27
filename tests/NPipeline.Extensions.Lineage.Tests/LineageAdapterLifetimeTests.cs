using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Extensions.Lineage.Tests;

/// <summary>
///     The lineage adapter reads a transform's input on a pump. The pump used to start as soon as the node was set up,
///     so when nothing read the transform's output (a sink that returns early) it kept pulling the upstream after the
///     run ended, and could still be inside the upstream stream when the context disposed it.
/// </summary>
public sealed class LineageAdapterLifetimeTests
{
    private const string CounterKey = "testing.upstream.counter";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Run_WhenSinkNeverReadsItsInput_DoesNotRunUpstreamAfterTheRun(bool upstreamRestarts)
    {
        // Arrange
        var counter = new Counter();
        var runner = new PipelineRunnerBuilder().UseLineage().Build();
        var context = new PipelineContext();
        context.Items[CounterKey] = counter;
        context.Items[nameof(upstreamRestarts)] = upstreamRestarts;

        // Act
        await runner.RunAsync<NeverReadingSinkPipeline>(context);
        var afterRun = counter.Value;

        var dispose = async () => await context.DisposeAsync();

        // Assert
        await dispose.Should().NotThrowAsync();
        await Task.Delay(200);
        counter.Value.Should().Be(afterRun, "no upstream work may happen once the run has returned");
        afterRun.Should().Be(0, "nothing read the transforms' output, so nothing should have pulled their input");
    }

    [Fact]
    public async Task Run_WhenSinkReadsEverything_ProcessesEveryItem()
    {
        // Arrange
        var counter = new Counter();
        var runner = new PipelineRunnerBuilder().UseLineage().Build();
        await using var context = new PipelineContext();
        context.Items[CounterKey] = counter;
        context.Items["upstreamRestarts"] = true;

        // Act
        await runner.RunAsync<ReadingSinkPipeline>(context);

        // Assert
        counter.Value.Should().Be(40);
    }

    private sealed class Counter
    {
        private int _value;

        public int Value => Volatile.Read(ref _value);

        public void Increment() => Interlocked.Increment(ref _value);
    }

    private sealed class NumberSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataStream<int>([.. Enumerable.Range(1, 40)], "numbers");
        }
    }

    private sealed class CountingTransform : TransformNode<int, int>
    {
        public override async ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(5, cancellationToken);
            ((Counter)context.Items[CounterKey]).Increment();
            return item;
        }
    }

    private sealed class PassThroughTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(item);
        }
    }

    private sealed class NeverReadingSink : SinkNode<int>
    {
        public override Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class ReadingSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }

    private static void DefineChain<TSink>(PipelineBuilder builder, PipelineContext context)
        where TSink : SinkNode<int>, new()
    {
        builder.EnableItemLevelLineage();

        var source = builder.AddSource<NumberSource, int>("source");
        var upstream = builder.AddTransform<CountingTransform, int, int>("upstream");
        var downstream = builder.AddTransform<PassThroughTransform, int, int>("downstream");
        var sink = builder.AddSink<TSink, int>("sink");

        if (context.Items.TryGetValue("upstreamRestarts", out var restarts) && restarts is true)
            builder.WithResilience(upstream, o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 3, MaxReplayWindow = 100 } });

        builder.Connect(source, upstream);
        builder.Connect(upstream, downstream);
        builder.Connect(downstream, sink);
    }

    private sealed class NeverReadingSinkPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context) => DefineChain<NeverReadingSink>(builder, context);
    }

    private sealed class ReadingSinkPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context) => DefineChain<ReadingSink>(builder, context);
    }
}
