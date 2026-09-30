using Xunit;
using AwesomeAssertions;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Sql;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Tests.Sql;

public sealed class SqlSinkNodeTests
{
    private readonly FakeDatabase _database = new();

    private TestSink<Order> Sink(Func<TestWriteOptions, TestWriteOptions>? configure = null, int maxParameters = 100) =>
        new((configure ?? (o => o))(new TestWriteOptions { Database = _database, Table = "orders" }), new TestDialect(maxParameters));

    [Fact]
    public async Task Writes_each_batch_as_multi_row_inserts_under_the_parameter_limit()
    {
        // Three columns and a limit of 7 parameters: two rows per statement.
        await SqlNodeRunner.WriteAsync(Sink(o => o with { BatchSize = 3 }, 7), Enumerable.Range(1, 5).Select(Order.Create));

        _database.Committed.Select(s => s.Sql).Should().Equal(
            "INSERT INTO [orders] ([Id], [Status], [Note]) VALUES (@p0, @p1, @p2), (@p3, @p4, @p5)",
            "INSERT INTO [orders] ([Id], [Status], [Note]) VALUES (@p0, @p1, @p2)",
            "INSERT INTO [orders] ([Id], [Status], [Note]) VALUES (@p0, @p1, @p2), (@p3, @p4, @p5)");

        // Enums go as their underlying integer and null as NULL.
        _database.Committed[0].Values.Should().Equal(1, 1, "note 1", 2, 0, null);
    }

    [Fact]
    public async Task Writes_rows_one_statement_each_when_asked()
    {
        await SqlNodeRunner.WriteAsync(Sink(o => o with { PerRow = true }), [Order.Create(1), Order.Create(2)]);

        _database.Committed.Should().HaveCount(2);
        _database.Committed.Should().OnlyContain(s => s.Sql == "INSERT INTO [orders] ([Id], [Status], [Note]) VALUES (@p0, @p1, @p2)");
        _database.Committed.Select(s => s.Values[0]).Should().Equal(1, 2);
    }

    [Fact]
    public async Task Writes_an_upsert_on_the_keys()
    {
        await SqlNodeRunner.WriteAsync(Sink(o => o with { Upsert = SqlUpsert.On("Id") }), [Order.Create(1)]);

        _database.Committed.Single().Sql.Should().Be("UPSERT [orders] ([Id], [Status], [Note]) ON ([Id]) Update VALUES (@p0, @p1, @p2)");
    }

    [Fact]
    public async Task By_default_each_batch_is_its_own_transaction()
    {
        await SqlNodeRunner.WriteAsync(Sink(o => o with { BatchSize = 2 }), Enumerable.Range(1, 5).Select(Order.Create));

        _database.Begun.Should().Be(3);
        _database.Commits.Should().Be(3);
    }

    [Fact]
    public async Task A_failed_batch_is_rolled_back_and_retried_whole()
    {
        _database.Fail = (n, _) => n == 2 ? new TransientFakeException("blip") : null;

        await SqlNodeRunner.WriteAsync(Sink(o => o with { BatchSize = 2, Attempts = 3 }, 4), Enumerable.Range(1, 4).Select(Order.Create));

        // Batch one is two statements (a limit of 4 parameters fits one row); its second failed, so it was rolled back and run again.
        _database.Rollbacks.Should().Be(1);
        _database.Committed.Select(s => s.Values[0]).Should().Equal(1, 2, 3, 4);
    }

    [Fact]
    public async Task Without_a_transaction_a_failed_batch_is_not_retried()
    {
        _database.Fail = (n, _) => n == 1 ? new TransientFakeException("blip") : null;

        var write = () => SqlNodeRunner.WriteAsync(Sink(o => o with { Transaction = SqlTransactionMode.None, Attempts = 3 }), [Order.Create(1)]);

        await write.Should().ThrowAsync<TransientFakeException>();
        _database.Executed.Should().ContainSingle();
    }

    [Fact]
    public async Task A_whole_run_transaction_undoes_every_batch_when_one_fails()
    {
        _database.Fail = (_, s) => s.Values.Contains(3) ? new InvalidOperationException("duplicate key") : null;

        var write = () => SqlNodeRunner.WriteAsync(Sink(o => o with { Transaction = SqlTransactionMode.WholeRun, BatchSize = 2 }), Enumerable.Range(1, 4).Select(Order.Create));

        await write.Should().ThrowAsync<InvalidOperationException>();
        _database.Begun.Should().Be(1);
        _database.Rollbacks.Should().Be(1);
        _database.Committed.Should().BeEmpty();
    }

    [Fact]
    public async Task A_failed_batch_can_go_to_the_dead_letter_sink()
    {
        _database.Fail = (_, s) => s.Values.Contains(3) ? new InvalidOperationException("duplicate key") : null;
        var deadLetters = new CapturingDeadLetterSink();
        var sink = Sink(o => o with { BatchSize = 2, FailedBatches = FailedBatchAction.DeadLetter });

        await PipelineRunner.Create().RunAsync(new SinkPipeline(sink, deadLetters, Enumerable.Range(1, 6).Select(Order.Create).ToArray()), new PipelineContext());

        var failure = deadLetters.Captured.Should().ContainSingle().Subject.Item.Should().BeOfType<SqlBatchFailure<Order>>().Subject;
        failure.Table.Should().Be("orders");
        failure.Items.Select(o => o.Id).Should().Equal(3, 4);
        _database.Committed.Select(s => s.Values[0]).Should().Equal(1, 5);
    }

    [Fact]
    public async Task Identifiers_are_quoted_and_escaped()
    {
        var sink = new TestSink<Oddly>(new TestWriteOptions { Database = _database, Table = "we]ird", Schema = "s", ValidateIdentifiers = false });

        await SqlNodeRunner.WriteAsync(sink, [new Oddly { Value = 1 }]);

        _database.Committed.Single().Sql.Should().StartWith("INSERT INTO [s].[we]]ird] ([co]]l]) VALUES");
    }

    [Fact]
    public void Plain_identifiers_are_required_unless_validation_is_off()
    {
        var create = () => Sink(o => o with { Table = "orders; DROP TABLE users" });

        create.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Options_are_validated()
    {
        var both = () => new TestSink<Order>(new TestWriteOptions { Database = _database, ConnectionString = "x", Table = "orders" });
        var deadLetterWholeRun = () => Sink(o => o with { FailedBatches = FailedBatchAction.DeadLetter, Transaction = SqlTransactionMode.WholeRun });
        var unknownKey = () => Sink(o => o with { Upsert = SqlUpsert.On("Nope") });
        var zeroBatch = () => Sink(o => o with { BatchSize = 0 });

        both.Should().Throw<ArgumentException>().WithMessage("*exactly one connection*");
        deadLetterWholeRun.Should().Throw<ArgumentException>();
        unknownKey.Should().Throw<ArgumentException>().WithMessage("*Nope*");
        zeroBatch.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void A_member_that_is_not_a_single_value_is_rejected()
    {
        var create = () => new TestSink<Nested>(new TestWriteOptions { Database = _database, Table = "t" });

        create.Should().Throw<NotSupportedException>().WithMessage("*Tags (List`1)*");
    }

    [Fact]
    public async Task Applies_the_naming_policy()
    {
        await SqlNodeRunner.WriteAsync(Sink(o => o with { Naming = ColumnNamingPolicy.SnakeCaseLower }), [Order.Create(1)]);

        _database.Committed.Single().Sql.Should().StartWith("INSERT INTO [orders] ([id], [status], [note])");
    }

    public enum Status
    {
        Pending,
        Active,
    }

    public sealed record Order
    {
        public int Id { get; init; }
        public Status Status { get; init; }
        public string? Note { get; init; }

        public static Order Create(int i) => new() { Id = i, Status = (Status)(i % 2), Note = i % 2 == 0 ? null : $"note {i}" };
    }

    public sealed class Oddly
    {
        [Column("co]l")]
        public int Value { get; set; }
    }

    public sealed class Nested
    {
        public List<string> Tags { get; set; } = [];
    }

    private sealed class SinkPipeline(TestSink<Order> sink, IDeadLetterSink deadLetters, Order[] items) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource(() => items, "orders-in");
            builder.Connect(source, builder.AddSink(sink, "orders"));
            builder.AddDeadLetterSink(deadLetters);
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
