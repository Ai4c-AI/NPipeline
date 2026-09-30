using NPipeline.Connectors.Files;
using NPipeline.Connectors.Mapping;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using Parquet;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet.Tests;

public sealed class ParquetSinkNodeTests : ParquetTestBase
{
    [Fact]
    public void Maps_member_types_to_parquet_columns()
    {
        var fields = Sink<AllTypes>().Schema.DataFields.ToDictionary(f => f.Name);

        fields["Int"].ClrType.Should().Be<int>();
        fields["Int"].IsNullable.Should().BeFalse();
        fields["MaybeInt"].IsNullable.Should().BeTrue();
        fields["UInt"].ClrType.Should().Be<uint>();
        fields["Key"].ClrType.Should().Be<Guid>();
        fields["Tier"].Should().BeOfType<DataField<string>>();
        fields["Char"].Should().BeOfType<DataField<string>>();
        fields["Text"].IsNullable.Should().BeTrue("strings are references and may be null");
        fields["Span"].ClrType.Should().Be<long>();

        fields["At"].Should().BeOfType<DateTimeDataField>().Which.Should().Match<DateTimeDataField>(f =>
            f.DateTimeFormat == DateTimeFormat.Timestamp && f.IsAdjustedToUTC && f.Unit == DateTimeTimeUnit.Micros);

        fields["Offset"].Should().BeOfType<DateTimeDataField>();
        fields["Time"].Should().BeOfType<TimeDataField>();
        fields["Decimal"].Should().BeOfType<DecimalDataField>().Which.Should().Match<DecimalDataField>(f => f.Precision == 38 && f.Scale == 18);
        fields["Money"].Should().BeOfType<DecimalDataField>().Which.Should().Match<DecimalDataField>(f => f.Precision == 10 && f.Scale == 3);
    }

    [Fact]
    public void Maps_collections_to_list_columns()
    {
        var fields = Sink<ListRow>().Schema.Fields.ToDictionary(f => f.Name);

        fields["Scores"].Should().BeOfType<ListField>().Which.Item.Should().BeAssignableTo<DataField>().Which.IsNullable.Should().BeFalse();
        fields["Tags"].Should().BeOfType<ListField>().Which.Item.Should().BeAssignableTo<DataField>().Which.IsNullable.Should().BeTrue();
        fields["Times"].Should().BeOfType<ListField>().Which.Item.Should().BeOfType<DateTimeDataField>();
    }

    [Fact]
    public void Honours_column_and_ignore_attributes_with_the_shared_attribute_first()
    {
        var names = Sink<AttributedOrder>().Schema.Fields.Select(f => f.Name);

        names.Should().Equal("order_id", "customer_name", "shared_wins");
    }

    [Fact]
    public void Applies_a_naming_policy()
    {
        var names = Sink<Order>(o => o with { Naming = ColumnNamingPolicy.SnakeCaseLower }).Schema.Fields.Select(f => f.Name);

        names.Should().Equal("id", "customer", "amount", "region");
    }

    [Fact]
    public void Rejects_members_with_no_parquet_column_naming_them()
    {
        var create = () => Sink<Nested>();

        create.Should().Throw<NotSupportedException>().WithMessage("*Inner (Order)*");
    }

    [Fact]
    public async Task Writes_a_scalar_type_as_one_value_column()
    {
        await PutAsync([1, 2, 3]);

        var (schema, _, _) = await InspectAsync();

        schema.Fields.Should().ContainSingle().Which.Name.Should().Be("Value");
        (await ReadAsync(Source<int>())).Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task Flushes_a_row_group_every_row_group_size_rows()
    {
        await PutAsync(Enumerable.Range(0, 25).Select(Order.Create), configure: o => o with { RowGroupSize = 10 });

        var (_, groups, _) = await InspectAsync();

        groups.Should().Equal(10, 10, 5);
    }

    [Fact]
    public async Task Flushes_a_row_group_when_its_buffered_bytes_reach_the_limit()
    {
        // About 1 KB per row; sizes are checked every 256 rows, so a 64 KB limit cuts groups at 256 rows.
        var rows = Enumerable.Range(0, 1000).Select(i => Order.Create(i) with { Customer = new string('x', 1024) });

        await PutAsync(rows, configure: o => o with { RowGroupBytes = 64 * 1024 });

        var (_, groups, _) = await InspectAsync();

        groups.Should().Equal(256, 256, 256, 232);
    }

    [Theory]
    [InlineData(CompressionMethod.None)]
    [InlineData(CompressionMethod.Snappy)]
    [InlineData(CompressionMethod.Gzip)]
    [InlineData(CompressionMethod.Zstd)]
    public async Task Writes_with_the_chosen_codec(CompressionMethod codec)
    {
        var rows = Enumerable.Range(0, 100).Select(Order.Create).ToList();

        await PutAsync(rows, configure: o => o with { Codec = codec });

        (await InspectAsync()).Codec.Should().Be(codec);
        (await ReadAsync(Source<Order>())).Should().Equal(rows);
    }

    [Fact]
    public async Task An_empty_input_writes_a_valid_file_with_the_schema()
    {
        await PutAsync(Array.Empty<Order>());

        var (schema, groups, _) = await InspectAsync();

        schema.Fields.Should().HaveCount(4);
        groups.Should().BeEmpty();
        (await ReadAsync(Source<Order>())).Should().BeEmpty();
    }

    [Fact]
    public async Task Null_items_fail_the_write_unless_skipped()
    {
        var write = () => PutAsync<Order?>([Order.Create(1), null]);
        await write.Should().ThrowAsync<Exception>();

        await PutAsync<Order?>([Order.Create(1), null, Order.Create(2)], configure: o => o with { NullItems = NullItemHandling.Skip });
        (await ReadAsync(Source<Order>())).Select(o => o.Id).Should().Equal(1, 2);
    }

    [Fact]
    public async Task Cancelling_a_write_fails_it()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await using var input = new InMemoryDataStream<Order>([Order.Create(1)]);
        var write = () => Sink<Order>().ConsumeAsync(input, new PipelineContext(), cts.Token);

        await write.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task On_the_file_system_a_file_is_written_under_a_temporary_name_and_moved_into_place()
    {
        var directory = Directory.CreateTempSubdirectory("npipeline-parquet-");

        try
        {
            var target = StorageUri.FromFilePath(Path.Combine(directory.FullName, "orders.parquet"));
            await WriteAsync(ParquetConnector.Sink<Order>(target), Enumerable.Range(0, 10).Select(Order.Create));

            directory.GetFiles().Select(f => f.Name).Should().Equal("orders.parquet");
            (await ReadAsync(ParquetConnector.Source<Order>(target))).Should().HaveCount(10);

            // A failed write leaves neither the target nor a temporary file.
            var failing = StorageUri.FromFilePath(Path.Combine(directory.FullName, "failed.parquet"));
            var write = () => WriteAsync(ParquetConnector.Sink<Order>(failing), Throwing());
            await write.Should().ThrowAsync<InvalidOperationException>();

            directory.GetFiles().Select(f => f.Name).Should().Equal("orders.parquet");
        }
        finally
        {
            directory.Delete(true);
        }

        static IEnumerable<Order> Throwing()
        {
            yield return Order.Create(1);
            throw new InvalidOperationException("boom");
        }
    }

    [Fact]
    public void Options_are_validated()
    {
        var zeroRows = () => Sink<Order>(o => o with { RowGroupSize = 0 });
        var zeroBytes = () => Sink<Order>(o => o with { RowGroupBytes = 0 });

        zeroRows.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("RowGroupSize");
        zeroBytes.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("RowGroupBytes");
    }

    public sealed class Nested
    {
        public int Id { get; set; }

        public Order Inner { get; set; } = new();
    }
}
