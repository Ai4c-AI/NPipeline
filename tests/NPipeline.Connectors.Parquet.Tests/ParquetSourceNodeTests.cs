using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Mapping;
using NPipeline.Pipeline;
using Parquet;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet.Tests;

public sealed class ParquetSourceNodeTests : ParquetTestBase
{
    [Fact]
    public async Task Round_trips_every_scalar_type()
    {
        var rows = Enumerable.Range(0, 25).Select(AllTypes.Create).ToList();

        await PutAsync(rows, configure: o => o with { RowGroupSize = 10 });

        var read = await ReadAsync(Source<AllTypes>());

        read.Should().BeEquivalentTo(rows, o => o.WithStrictOrdering());
        read.Should().OnlyContain(r => r.At.Kind == DateTimeKind.Utc);
    }

    [Fact]
    public async Task Round_trips_lists_that_are_null_empty_or_hold_nulls()
    {
        ListRow[] rows =
        [
            new() { Id = 1, Scores = [1, 2, 3], Tags = ["a", null, "c"], Times = [null, DateTime.UnixEpoch], Amounts = [1.5m], Keys = [Guid.Empty], Tiers = [Tier.Gold], Clock = [TimeOnly.MinValue] },
            new() { Id = 2, Scores = [], Tags = [], Times = [], Amounts = [], Keys = [], Tiers = [], Clock = [] },
            new() { Id = 3, Scores = [], Tags = null, Times = null, Amounts = null, Keys = null, Tiers = null, Clock = null },
            new() { Id = 4, Scores = [42], Tags = [null], Times = [null], Amounts = [0m, -1m], Keys = [Guid.Empty, Guid.Empty], Tiers = [Tier.Bronze, Tier.Silver], Clock = [new TimeOnly(23, 59)] },
        ];

        // A row group of three splits the rows across two groups, so level decoding restarts mid-table.
        await PutAsync(rows, configure: o => o with { RowGroupSize = 3 });

        var read = await ReadAsync(Source<ListRow>());

        read.Should().BeEquivalentTo(rows, o => o.WithStrictOrdering());
    }

    [Fact]
    public async Task Maps_columns_to_members_by_name_ignoring_case()
    {
        await PutAsync([new { ID = 1, CUSTOMER = "Ada", amount = 2.5m }]);

        var row = (await ReadAsync(Source<Order>())).Single();

        row.Should().Be(new Order { Id = 1, Customer = "Ada", Amount = 2.5m });
    }

    [Fact]
    public async Task Builds_positional_records_through_their_constructor()
    {
        await PutAsync(Enumerable.Range(1, 2).Select(Order.Create));

        var rows = await ReadAsync(Source<PositionalOrder>());

        rows.Should().Equal(new PositionalOrder(1, "c1"), new PositionalOrder(2, "c2"));
    }

    [Fact]
    public async Task Honours_column_and_ignore_attributes()
    {
        await PutAsync([new AttributedOrder { Id = 7, Customer = "Ada", Both = "b", Secret = "s", Internal = "i" }]);

        var row = (await ReadAsync(Source<AttributedOrder>())).Single();

        row.Should().BeEquivalentTo(new AttributedOrder { Id = 7, Customer = "Ada", Both = "b" });
    }

    [Fact]
    public async Task Applies_a_naming_policy()
    {
        await PutAsync([Order.Create(3)], configure: o => o with { Naming = ColumnNamingPolicy.SnakeCaseLower });

        var row = (await ReadAsync(Source<Order>(o => o with { Naming = ColumnNamingPolicy.SnakeCaseLower }))).Single();

        row.Should().Be(Order.Create(3));
    }

    [Fact]
    public async Task A_missing_optional_column_keeps_the_member_initialiser()
    {
        await PutAsync([new { Id = 1, Customer = "Ada", Amount = 1m }]);

        (await ReadAsync(Source<Order>())).Single().Region.Should().Be("AU");
    }

    [Fact]
    public async Task A_missing_required_column_fails_naming_it()
    {
        await PutAsync([new { Customer = "Ada" }]);

        var read = () => ReadAsync(Source<RequiredOrder>());

        (await read.Should().ThrowAsync<RecordBindingException>()).Which.MissingColumns.Should().Equal("Id");
    }

    [Fact]
    public async Task Missing_columns_can_be_made_an_error()
    {
        await PutAsync([new { Id = 1, Customer = "Ada" }]);

        var read = () => ReadAsync(Source<Order>(o => o with { MissingColumns = MissingColumnBehavior.Throw }));

        (await read.Should().ThrowAsync<RecordBindingException>()).Which.MissingColumns.Should().BeEquivalentTo(["Amount", "Region"]);
    }

    [Fact]
    public async Task Converts_between_compatible_column_and_member_types()
    {
        // An older file: narrower numbers, a required int, text for a number, and a date for a timestamp.
        await PutAsync([new { Id = 1, Score = 1.5f, Count = 3, Amount = "12.50", Day = new DateOnly(2025, 1, 2) }]);

        var row = (await ReadAsync(Source<Widened>())).Single();

        row.Should().BeEquivalentTo(new Widened { Id = 1, Score = 1.5, Count = 3, Amount = 12.5m, Day = new DateTime(2025, 1, 2, 0, 0, 0, DateTimeKind.Utc) });
    }

    [Fact]
    public async Task A_null_for_a_non_nullable_member_is_a_row_error()
    {
        await PutAsync([new Maybe { Id = 1, Score = 5 }, new Maybe { Id = 2, Score = null }, new Maybe { Id = 3, Score = 7 }]);

        var fail = () => ReadAsync(Source<Required>());
        var error = (await fail.Should().ThrowAsync<RecordMappingException>()).Which;
        error.RecordNumber.Should().Be(2);
        error.RawExcerpt.Should().Contain("Id=2");

        var errors = new List<RowError>();

        var rows = await ReadAsync(Source<Required>(o => o with
        {
            RowErrorHandler = e =>
            {
                errors.Add(e);
                return RowErrorAction.Skip;
            },
        }));

        rows.Select(r => r.Score).Should().Equal(5, 7);
        errors.Should().ContainSingle().Which.Field.Should().Be("Score");
    }

    [Fact]
    public async Task A_schema_validator_can_reject_a_file()
    {
        await PutAsync([Order.Create(1)]);

        var read = () => ReadAsync(Source<Order>(o => o with { SchemaValidator = schema => schema.Fields.Any(f => f.Name == "Missing") }));

        await read.Should().ThrowAsync<ParquetSchemaException>().WithMessage("*SchemaValidator*");
    }

    [Fact]
    public async Task A_row_filter_drops_rows_before_mapping()
    {
        await PutAsync(Enumerable.Range(0, 10).Select(Order.Create));

        var rows = await ReadAsync(Source<Order>(o => o with { RowFilter = row => row.Get<int>("Id") % 3 == 0 }));

        rows.Select(r => r.Id).Should().Equal(0, 3, 6, 9);
    }

    [Fact]
    public async Task A_row_group_filter_skips_groups_by_their_statistics()
    {
        await PutAsync(Enumerable.Range(0, 30).Select(Order.Create), configure: o => o with { RowGroupSize = 10 });
        var seen = new List<(int Index, long Rows, int Min, int Max)>();

        var rows = await ReadAsync(Source<Order>(o => o with
        {
            RowGroupFilter = group =>
            {
                group.TryGetRange<int>("id", out var min, out var max).Should().BeTrue();
                seen.Add((group.Index, group.RowCount, min, max));
                return max >= 15 && min <= 15;
            },
        }));

        seen.Should().Equal((0, 10, 0, 9), (1, 10, 10, 19), (2, 10, 20, 29));
        rows.Select(r => r.Id).Should().Equal(Enumerable.Range(10, 10));
    }

    [Fact]
    public async Task Row_group_ranges_decode_dates_and_times_and_skip_unsigned_or_missing_columns()
    {
        await PutAsync([AllTypes.Create(1)]);
        bool? unsigned = null, missing = null, stamps = null, days = null, times = null;
        var row = AllTypes.Create(1);

        _ = await ReadAsync(Source<AllTypes>(o => o with
        {
            RowGroupFilter = group =>
            {
                unsigned = group.TryGetRange<uint>("UInt", out _, out _);
                missing = group.TryGetRange<int>("Nope", out _, out _);

                // Parquet.Net records timestamps to the millisecond; the range still holds the value.
                stamps = group.TryGetRange<DateTime>("At", out var from, out var to) && from <= row.At && row.At <= to && to - from < TimeSpan.FromMilliseconds(1);
                days = group.TryGetRange<DateOnly>("Day", out var first, out var last) && first == row.Day && last == row.Day;
                times = group.TryGetRange<TimeOnly>("Time", out var early, out var late) && early == row.Time && late == row.Time;
                return true;
            },
        }));

        unsigned.Should().BeFalse();
        missing.Should().BeFalse();
        stamps.Should().BeTrue();
        days.Should().BeTrue();
        times.Should().BeTrue();
    }

    [Fact]
    public async Task Fills_members_from_partition_directories()
    {
        await PutAsync([new { Id = 1 }], "sales/region=EU/day=2025-01-02/part-0.parquet");
        await PutAsync([new { Id = 2 }], "sales/region=__HIVE_DEFAULT_PARTITION__/day=2025-01-03/part-0.parquet");

        var rows = await ReadAsync(Source<Partitioned>(path: "sales/", configure: o => o with { Recursive = true }));

        rows.Should().BeEquivalentTo([
            new Partitioned { Id = 1, Region = "EU", Day = new DateOnly(2025, 1, 2) },
            new Partitioned { Id = 2, Region = null, Day = new DateOnly(2025, 1, 3) },
        ]);

        var ignored = await ReadAsync(Source<Partitioned>(path: "sales/", configure: o => o with { Recursive = true, PartitionColumns = false }));
        ignored.Should().OnlyContain(r => r.Region == null && r.Day == default);
    }

    [Fact]
    public async Task A_column_in_the_file_wins_over_a_partition_directory()
    {
        await PutAsync([new { Id = 1, Region = "file" }], "t/region=path/part.parquet");

        var row = (await ReadAsync(Source<Partitioned>(path: "t/", configure: o => o with { Recursive = true }))).Single();

        row.Region.Should().Be("file");
    }

    [Fact]
    public async Task Reads_a_directory_in_path_order_skipping_other_files()
    {
        await PutAsync([Order.Create(2)], "dir/b.parquet");
        await PutAsync([Order.Create(1)], "dir/a.parquet");
        Provider.Put(Uri("dir/readme.txt"), "not parquet"u8.ToArray());

        var rows = await ReadAsync(Source<Order>(path: "dir/"));

        rows.Select(r => r.Id).Should().Equal(1, 2);
    }

    [Fact]
    public async Task Reads_files_in_parallel_in_path_order()
    {
        for (var file = 0; file < 12; file++)
        {
            await PutAsync(Enumerable.Range(file * 100, 100).Select(Order.Create), $"many/f{file:D2}.parquet", o => o with { RowGroupSize = 30 });
        }

        var rows = await ReadAsync(Source<Order>(path: "many/", configure: o => o with { FileReadParallelism = 4 }));

        rows.Select(r => r.Id).Should().Equal(Enumerable.Range(0, 1200));
    }

    [Fact]
    public async Task Reads_from_a_stream_that_cannot_seek()
    {
        var provider = new NPipeline.Tests.Common.InMemoryStorageProvider { NonSeekableReads = true };
        var uri = Uri();
        await WriteAsync(ParquetConnector.Sink<Order>(uri, o => o with { Provider = provider }), Enumerable.Range(0, 5).Select(Order.Create));

        var rows = await ReadAsync(ParquetConnector.Source<Order>(uri, o => o with { Provider = provider }));

        rows.Should().HaveCount(5);
    }

    [Fact]
    public async Task Reads_timestamps_from_other_writers_as_utc()
    {
        // Parquet.Net writes a plain DataField<DateTime> as INT96, which reads back without a kind.
        var schema = new ParquetSchema(new DataField<int>("Id"), new DataField<DateTime>("At"), new TimeDataField("Time", TimeUnitPrecision.Millis));
        var at = new DateTime(2025, 6, 7, 8, 9, 10);

        await PutRawAsync("raw.parquet", schema, async group =>
        {
            await group.WriteAsync<int>(schema.DataFields[0], new[] { 1 });
            await group.WriteAsync<DateTime>(schema.DataFields[1], new[] { at });
            await group.WriteAsync<int>(schema.DataFields[2], new[] { 3_723_004 });
        });

        var row = (await ReadAsync(Source<Stamped>(path: "raw.parquet"))).Single();

        row.At.Should().Be(DateTime.SpecifyKind(at, DateTimeKind.Utc));
        row.At.Kind.Should().Be(DateTimeKind.Utc);
        row.Time.Should().Be(new TimeOnly(1, 2, 3, 4));
    }

    [Fact]
    public async Task A_manual_mapper_gets_every_column_and_its_errors_are_row_errors()
    {
        await PutAsync(Enumerable.Range(1, 3).Select(Order.Create));

        var rows = await ReadAsync(Source(
            row => row.Get<int>("Id") == 2 ? throw new InvalidOperationException("bad") : $"{row.Get<string>("Customer")}:{row.ColumnCount}:{row.RecordNumber}",
            o => o with { RowErrorHandler = _ => RowErrorAction.Skip }));

        rows.Should().Equal("c1:4:1", "c3:4:3");
    }

    [Fact]
    public async Task Projected_columns_limit_what_a_manual_mapper_sees()
    {
        await PutAsync([Order.Create(1)]);

        var row = (await ReadAsync(Source(r => r, o => o with { ProjectedColumns = ["Id", "Region"] }))).Single();

        row.ColumnNames.Should().Equal("Id", "Region");
        row.HasColumn("Customer").Should().BeFalse();
        row.Schema.Fields.Should().HaveCount(4, "the schema is the file's");
    }

    [Fact]
    public async Task Parquet_rows_convert_values_on_request()
    {
        await PutAsync([AllTypes.Create(1), AllTypes.Create(2)]);

        var rows = await ReadAsync(Source(r => r));
        var odd = rows[0];
        var even = rows[1];

        odd.Get<long>("Int").Should().Be(int.MinValue + 1);
        odd.Get<string>("Tier").Should().Be("Silver");
        odd.Get<Tier>("tier").Should().Be(Tier.Silver);
        odd.Get<DateTimeOffset>("At").Should().Be(new DateTimeOffset(AllTypes.Create(1).At));
        odd.Get<TimeSpan>("Span").Should().Be(AllTypes.Create(1).Span);
        odd.Get<TimeOnly>("Time").Should().Be(AllTypes.Create(1).Time);
        odd.Get<DateOnly>("Day").Should().Be(AllTypes.Create(1).Day);
        odd.Get<int?>("MaybeInt").Should().Be(1);

        even.IsNull("MaybeInt").Should().BeTrue();
        even.Get<int?>("MaybeInt").Should().BeNull();
        even.GetOrDefault("MaybeInt", -1).Should().Be(-1);
        even.TryGet<int>("Nope", out _).Should().BeFalse();

        var strictNull = () => even.Get<int>("MaybeInt");
        var missing = () => even.Get<int>("Nope");
        strictNull.Should().Throw<FieldMappingException>();
        missing.Should().Throw<FieldMappingException>().WithMessage("*Nope*");
    }

    [Fact]
    public async Task Parquet_row_writer_writes_rows_back_with_their_schema()
    {
        var original = Enumerable.Range(0, 20).Select(AllTypes.Create).ToList();
        var lists = new[] { new ListRow { Id = 1, Scores = [1, 2], Tags = ["a", null], Times = null, Amounts = [], Clock = [new TimeOnly(23, 59, 59, 999, 999)] } };
        await PutAsync(original);
        await PutAsync(lists, "lists.parquet");

        foreach (var (from, to) in new[] { ("data.parquet", "copy.parquet"), ("lists.parquet", "lists-copy.parquet") })
        {
            var rows = await ReadAsync(Source(r => r, path: from));
            using var stream = new MemoryStream();
            await ParquetRowWriter.WriteAsync(stream, rows[0].Schema, rows, CompressionMethod.Zstd, rowGroupSize: 7);
            Provider.Put(Uri(to), stream.ToArray());
        }

        (await ReadAsync(Source<AllTypes>(path: "copy.parquet"))).Should().BeEquivalentTo(original, o => o.WithStrictOrdering());
        (await ReadAsync(Source<ListRow>(path: "lists-copy.parquet"))).Should().BeEquivalentTo(lists);
        (await InspectAsync("copy.parquet")).RowGroups.Should().Equal(7, 7, 6);
    }

    [Fact]
    public async Task Parquet_row_writer_rejects_nested_columns()
    {
        var schema = new ParquetSchema(new StructField("Inner", new DataField<int>("X")));

        var write = () => ParquetRowWriter.WriteAsync(new MemoryStream(), schema, []);

        await write.Should().ThrowAsync<NotSupportedException>().WithMessage("*Inner*");
    }

    [Fact]
    public async Task Cancelling_a_read_stops_it()
    {
        await PutAsync(Enumerable.Range(0, 100).Select(Order.Create));
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var read = async () =>
        {
            await foreach (var _ in Source<Order>().OpenStream(new PipelineContext(), cts.Token))
            {
            }
        };

        await read.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public void Options_are_validated()
    {
        var create = () => Source<Order>(o => o with { FileReadParallelism = 0 });

        create.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("FileReadParallelism");
    }

    public sealed class Widened
    {
        public long Id { get; set; }
        public double Score { get; set; }
        public int? Count { get; set; }
        public decimal Amount { get; set; }
        public DateTime Day { get; set; }
    }

    public sealed class Maybe
    {
        public int Id { get; set; }
        public int? Score { get; set; }
    }

    public sealed class Required
    {
        public int Id { get; set; }
        public int Score { get; set; }
    }

    public sealed class Partitioned
    {
        public int Id { get; set; }
        public string? Region { get; set; }
        public DateOnly Day { get; set; }
    }

    public sealed class Stamped
    {
        public int Id { get; set; }
        public DateTime At { get; set; }
        public TimeOnly Time { get; set; }
    }
}
