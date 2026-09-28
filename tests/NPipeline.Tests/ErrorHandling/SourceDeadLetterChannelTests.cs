using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.ErrorHandling;

public sealed class SourceDeadLetterChannelTests
{
    [Fact]
    public async Task Records_dead_lettered_by_a_source_are_attributed_to_its_node()
    {
        var deadLetters = new CapturingDeadLetterSink();
        var context = new PipelineContext();

        await PipelineRunner.Create().RunAsync(new Definition(deadLetters), context);

        var envelope = deadLetters.Captured.Should().ContainSingle().Subject;
        envelope.Item.Should().Be("raw record 2");
        envelope.Error.Should().BeOfType<FormatException>();
        envelope.Attribution.DecisionNodeId.Should().Be("rows");
        envelope.Attribution.OriginNodeId.Should().Be("rows");
        envelope.Attribution.RunId.Should().Be(context.RunIdentity.RunId);
        context.GetSink<InMemorySinkNode<int>>().Items.Should().BeEquivalentTo([1, 3]);
    }

    [Fact]
    public async Task Dead_lettering_without_a_sink_fails_the_run()
    {
        var run = () => PipelineRunner.Create().RunAsync(new Definition(null), new PipelineContext());

        var failure = await run.Should().ThrowAsync<Exception>();
        failure.Which.Should().Match<Exception>(e => Unwrap(e).Any(inner => inner is DeadLetterSinkNotConfiguredException));
    }

    [Fact]
    public void Outside_a_run_the_channel_is_attributed_to_the_node_type()
    {
        var source = new DeadLetteringSource();

        _ = source.OpenStream(new PipelineContext(), CancellationToken.None);

        source.Channel!.NodeId.Should().Be(nameof(DeadLetteringSource));
    }

    [Fact]
    public void The_node_scope_does_not_outlive_the_open()
    {
        using (SourceNodeScope.Enter("outer"))
        {
            using (SourceNodeScope.Enter("inner"))
            {
                SourceNodeScope.CurrentNodeId.Should().Be("inner");
            }

            SourceNodeScope.CurrentNodeId.Should().Be("outer");
        }

        SourceNodeScope.CurrentNodeId.Should().BeNull();
    }

    private static IEnumerable<Exception> Unwrap(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            yield return current;

            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions.SelectMany(Unwrap))
                {
                    yield return inner;
                }
            }
        }
    }

    private sealed class Definition(IDeadLetterSink? deadLetters) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource(new DeadLetteringSource(), "rows");
            var sink = builder.AddInMemorySink<int>(context);
            builder.Connect(source, sink);

            if (deadLetters is not null)
                builder.AddDeadLetterSink(deadLetters);
        }
    }

    /// <summary>Yields 1 and 3, and dead-letters the record in between, as a file source does with a row that fails to map.</summary>
    private sealed class DeadLetteringSource : SourceNode<int>
    {
        public SourceDeadLetterChannel? Channel { get; private set; }

        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            Channel = OpenDeadLetterChannel(context);
            return new DataStream<int>(Rows(Channel, cancellationToken));
        }

        private static async IAsyncEnumerable<int> Rows(SourceDeadLetterChannel channel, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            yield return 1;
            await channel.SendAsync("raw record 2", new FormatException("not a number"), cancellationToken);
            yield return 3;
        }
    }

    private sealed class CapturingDeadLetterSink : IDeadLetterSink
    {
        public List<DeadLetterEnvelope> Captured { get; } = [];

        public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
        {
            Captured.Add(envelope);
            return Task.CompletedTask;
        }
    }
}
