using AwesomeAssertions;
using NPipeline.Connectors.Errors;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Csv.Tests;

public sealed class CsvIntegrationTests : CsvTestBase
{
    [Fact]
    public async Task A_tap_writes_the_header_and_every_row_on_the_file_system()
    {
        var directory = Directory.CreateTempSubdirectory("np-csv-");

        try
        {
            var path = Path.Combine(directory.FullName, "rows.csv");
            var sink = CsvConnector.Sink<Row>(StorageUri.FromFilePath(path));

            await PipelineRunner.Create().RunAsync(new TapPipeline(sink), new PipelineContext());

            (await File.ReadAllLinesAsync(path)).Should().Equal("Id,Name", "1,alpha", "2,beta", "3,gamma");

            // Written through a temporary file that was moved into place.
            directory.GetFiles().Select(f => f.Name).Should().Equal("rows.csv");
        }
        finally
        {
            directory.Delete(true);
        }
    }

    [Fact]
    public async Task Dead_lettered_rows_reach_the_pipeline_dead_letter_sink()
    {
        Put("Id,FirstName,Amount\n1,Ada,1\n2,Grace,oops\n3,Joan,3\n");
        var deadLetters = new CapturingDeadLetterSink();
        var context = new PipelineContext();
        var source = Source<Person>(o => o with { RowErrorHandler = _ => RowErrorAction.DeadLetter });

        await PipelineRunner.Create().RunAsync(new DeadLetterPipeline(source, deadLetters), context);

        var envelope = deadLetters.Captured.Should().ContainSingle().Subject;
        envelope.Item.Should().Be(new ConnectorRecordFailure("mem://test/data.csv", 2, "Amount", "2,Grace,oops\n"));
        envelope.Attribution.DecisionNodeId.Should().Be("people");
        context.GetSink<InMemorySinkNode<Person>>().Items.Select(p => p.Id).Should().BeEquivalentTo([1, 3]);
    }

    private sealed record Row(int Id, string Name);

    private sealed class TapPipeline(CsvSinkNode<Row> csvSink) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource(() => new[] { new Row(1, "alpha"), new Row(2, "beta"), new Row(3, "gamma") }, "source");
            var tap = builder.AddTap<Row>(csvSink, "tap");
            var sink = builder.AddSink<CountingSink, Row>("sink");
            _ = builder.AddPreconfiguredNodeInstance(sink.Id, new CountingSink()).Connect(source, tap).Connect(tap, sink);
        }
    }

    private sealed class DeadLetterPipeline(CsvSourceNode<Person> source, IDeadLetterSink deadLetters) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var handle = builder.AddSource(source, "people");
            builder.Connect(handle, builder.AddInMemorySink<Person>(context));
            builder.AddDeadLetterSink(deadLetters);
        }
    }

    private sealed class CountingSink : SinkNode<Row>
    {
        public override async Task ConsumeAsync(IDataStream<Row> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
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
