using NPipeline.Connectors.Database.RoundTrip.Tests.Harnesses;
using NPipeline.Connectors.Database.RoundTrip.Tests.Infrastructure;
using NPipeline.Connectors.Database.RoundTrip.Tests.Models;

namespace NPipeline.Connectors.Database.RoundTrip.Tests;

/// <summary>
///     The scenarios every database connector must pass: write records through the sink into a table created with plain
///     SQL, read them back through the source, and expect the same records. Each database's test class runs them all.
/// </summary>
public abstract class DatabaseRoundTripTests(DatabaseHarness harness)
{
    protected DatabaseHarness Harness { get; } = harness;

    public static TheoryData<WriteMode> WriteModes => new() { WriteMode.PerRow, WriteMode.Batch, WriteMode.Bulk };

    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task Scalars(WriteMode mode)
    {
        if (!Harness.Supports(mode))
            return;

        var table = await Harness.CreateTableAsync(ScalarRow.Columns);
        var rows = Enumerable.Range(1, 25).Select(ScalarRow.Create).ToList();

        await Harness.WriteAsync(table, rows, new WriteSettings { Mode = mode, BatchSize = 10 });
        var read = await Harness.ReadAsync<ScalarRow>(Harness.SelectAll(table));

        read.Should().BeEquivalentTo(rows, o => o.WithStrictOrdering());
    }

    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task Nullables(WriteMode mode)
    {
        if (!Harness.Supports(mode))
            return;

        var table = await Harness.CreateTableAsync(NullableRow.Columns);

        NullableRow[] rows =
        [
            new() { Id = 1 },
            new() { Id = 2, Count = 7, Price = 12.3456m, Score = 0.5, Flag = true, SeenAt = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc), Note = "note" },
        ];

        await Harness.WriteAsync(table, rows, new WriteSettings { Mode = mode });

        (await Harness.ReadAsync<NullableRow>(Harness.SelectAll(table))).Should().BeEquivalentTo(rows, o => o.WithStrictOrdering());
    }

    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task Guids_dates_times_and_binary(WriteMode mode)
    {
        if (!Harness.Supports(mode))
            return;

        var table = await Harness.CreateTableAsync(ExtendedRow.Columns);
        var rows = Enumerable.Range(1, 5).Select(ExtendedRow.Create).ToList();

        await Harness.WriteAsync(table, rows, new WriteSettings { Mode = mode });

        (await Harness.ReadAsync<ExtendedRow>(Harness.SelectAll(table))).Should().BeEquivalentTo(rows, o => o.WithStrictOrdering());
    }

    [Fact]
    public async Task DateTimeOffsets_keep_their_instant()
    {
        if (!Harness.Supports(ColumnType.DateTimeOffset))
            return;

        var table = await Harness.CreateTableAsync(OffsetRow.Columns);

        OffsetRow[] rows =
        [
            new() { Id = 1, At = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.FromHours(5.5)) },
            new() { Id = 2, At = new DateTimeOffset(2026, 7, 8, 9, 10, 11, TimeSpan.FromHours(-8)) },
        ];

        await Harness.WriteAsync(table, rows);
        var read = await Harness.ReadAsync<OffsetRow>(Harness.SelectAll(table));

        read.Select(r => (r.Id, r.At.UtcDateTime)).Should().Equal(rows.Select(r => (r.Id, r.At.UtcDateTime)));
    }

    [Fact]
    public async Task Enums_as_integers()
    {
        var table = await Harness.CreateTableAsync(EnumRow.Columns);
        EnumRow[] rows = [new() { Id = 1, Status = Status.Active }, new() { Id = 2, Status = Status.Suspended, Optional = Status.Pending }];

        await Harness.WriteAsync(table, rows);

        (await Harness.ReadAsync<EnumRow>(Harness.SelectAll(table))).Should().BeEquivalentTo(rows, o => o.WithStrictOrdering());
    }

    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task Tricky_text(WriteMode mode)
    {
        if (!Harness.Supports(mode))
            return;

        var table = await Harness.CreateTableAsync(TextRow.Columns);
        var rows = TextRow.TrickyValues.Select((text, i) => new TextRow { Id = i, Text = text }).ToList();

        await Harness.WriteAsync(table, rows, new WriteSettings { Mode = mode });

        (await Harness.ReadAsync<TextRow>(Harness.SelectAll(table))).Should().BeEquivalentTo(rows, o => o.WithStrictOrdering());
    }

    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task Volume(WriteMode mode)
    {
        if (!Harness.Supports(mode))
            return;

        var table = await Harness.CreateTableAsync(ScalarRow.Columns);
        var count = mode == WriteMode.PerRow ? 500 : 5_000;
        var rows = Enumerable.Range(1, count).Select(ScalarRow.Create).ToList();

        await Harness.WriteAsync(table, rows, new WriteSettings { Mode = mode, BatchSize = 1_000 });

        (await Harness.CountAsync(table)).Should().Be(count);
        (await Harness.ReadAsync<ScalarRow>(Harness.SelectAll(table))).Should().BeEquivalentTo(rows, o => o.WithStrictOrdering());
    }

    [Fact]
    public async Task A_value_that_does_not_convert_fails_the_read_naming_the_column()
    {
        var table = await Harness.CreateTableAsync(new Column("Id", ColumnType.Int, Key: true), new Column("Score", ColumnType.Text));
        await Harness.ExecuteAsync($"INSERT INTO {Harness.Quote(table)} VALUES (1, 'abc')");

        var read = () => Harness.ReadAsync<StrictScoreRow>(Harness.SelectAll(table));

        (await read.Should().ThrowAsync<Exception>()).Which.ToString().Should().Contain(Harness.ColumnName("Score"));
    }

    [Fact]
    public async Task Positional_records_are_built_through_their_constructor()
    {
        var table = await Harness.CreateTableAsync(new Column("Id", ColumnType.Int, Key: true), new Column("Name", ColumnType.Text));
        await Harness.ExecuteAsync($"INSERT INTO {Harness.Quote(table)} VALUES (1, 'Ada')");

        (await Harness.ReadAsync<PositionalRow>(Harness.SelectAll(table))).Should().Equal(new PositionalRow(1, "Ada"));
    }

    [Fact]
    public async Task Text_numbers_convert_culture_invariantly()
    {
        // The environment is de-DE, where Convert.ChangeType reads "1.5" as 15.
        var table = await Harness.CreateTableAsync(new Column("Id", ColumnType.Int, Key: true), new Column("Amount", ColumnType.Text));
        await Harness.ExecuteAsync($"INSERT INTO {Harness.Quote(table)} VALUES (1, '1.5')");

        (await Harness.ReadAsync<AmountRow>(Harness.SelectAll(table))).Single().Amount.Should().Be(1.5m);
    }

    [Fact]
    public async Task A_row_that_fails_to_map_is_skipped_not_emitted_half_filled()
    {
        var table = await Harness.CreateTableAsync(new Column("Id", ColumnType.Int, Key: true), new Column("Score", ColumnType.Text));
        await Harness.ExecuteAsync($"INSERT INTO {Harness.Quote(table)} VALUES (1, 'abc'), (2, '5')");

        var read = await Harness.ReadAsync<ScoreRow>(Harness.SelectAll(table), new ReadSettings { ThrowOnMappingError = false });

        read.Should().Equal(new ScoreRow { Id = 2, Score = 5 });
    }

    [Fact]
    public async Task Each_source_maps_with_its_own_settings()
    {
        var table = await Harness.CreateTableAsync(new Column("Id", ColumnType.Int, Key: true), new Column("Score", ColumnType.Text));
        await Harness.ExecuteAsync($"INSERT INTO {Harness.Quote(table)} VALUES (1, 'abc')");

        // A lenient source for the type first, then a strict one: the strict one must still fail.
        _ = await Harness.ReadAsync<CachedScoreRow>(Harness.SelectAll(table), new ReadSettings { ThrowOnMappingError = false });
        var strict = () => Harness.ReadAsync<CachedScoreRow>(Harness.SelectAll(table));

        await strict.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public async Task A_batch_that_fails_to_write_is_not_dropped_silently()
    {
        var table = await Harness.CreateTableAsync(ScalarRow.Columns);
        await Harness.WriteAsync(table, [ScalarRow.Create(2)]);

        var write = () => Harness.WriteAsync(table, [ScalarRow.Create(1), ScalarRow.Create(2), ScalarRow.Create(3)],
            new WriteSettings { BatchSize = 1, ContinueOnError = true });

        await write.Should().ThrowAsync<Exception>("a lost batch must be reported, or sent to the dead-letter sink");
    }

    [Fact]
    public async Task UseTransaction_writes_everything_or_nothing()
    {
        var table = await Harness.CreateTableAsync(ScalarRow.Columns);

        // The second batch repeats a key, so it fails after the first batch was written.
        var write = () => Harness.WriteAsync(table, [ScalarRow.Create(1), ScalarRow.Create(2), ScalarRow.Create(3), ScalarRow.Create(1)],
            new WriteSettings { BatchSize = 2, UseTransaction = true });

        await write.Should().ThrowAsync<Exception>();
        (await Harness.CountAsync(table)).Should().Be(0);
    }

    [Fact]
    public async Task Identifiers_are_escaped_when_not_validated()
    {
        var table = await Harness.CreateTableAsync(new Column("Id", ColumnType.Int, Key: true), new Column(HostileNameRow.ColumnName, ColumnType.Text, Explicit: true));
        HostileNameRow[] rows = [new() { Id = 1, Value = "kept" }];

        await Harness.WriteAsync(table, rows, new WriteSettings { ValidateIdentifiers = false });

        (await Harness.ReadAsync<HostileNameRow>(Harness.SelectAll(table))).Should().BeEquivalentTo(rows);
    }

    [Fact]
    public async Task Values_bind_to_columns_by_name_not_position()
    {
        // The table's columns are in the opposite order to the record's members.
        var table = await Harness.CreateTableAsync(new Column("Name", ColumnType.Text), new Column("Id", ColumnType.Int, Key: true));

        await Harness.WriteAsync(table, [new PositionalRow(1, "ada")]);

        (await Harness.ReadAsync<PositionalRow>(Harness.SelectAll(table))).Should().Equal(new PositionalRow(1, "ada"));
    }
}

[Collection(SqlServerDatabases.Name)]
public sealed class SqlServerRoundTripTests(SqlServerContainerFixture database) : DatabaseRoundTripTests(new SqlServerHarness(database.ConnectionString));

[Collection(PostgresDatabases.Name)]
public sealed class PostgresRoundTripTests(PostgresContainerFixture database) : DatabaseRoundTripTests(new PostgresHarness(database.ConnectionString))
{
    [Fact]
    public async Task Acronyms_become_one_snake_case_word()
    {
        await Harness.ExecuteAsync("CREATE TABLE IF NOT EXISTS acronyms (id INTEGER PRIMARY KEY, http_status TEXT NOT NULL)");
        await Harness.ExecuteAsync("TRUNCATE acronyms");
        await Harness.ExecuteAsync("INSERT INTO acronyms VALUES (1, 'OK')");

        (await Harness.ReadAsync<AcronymRow>("SELECT * FROM acronyms")).Single().HTTPStatus.Should().Be("OK");
    }

    public sealed record AcronymRow
    {
        public int Id { get; init; }

        public string HTTPStatus { get; init; } = string.Empty;
    }
}

[Collection(MySqlDatabases.Name)]
public sealed class MySqlRoundTripTests(MySqlContainerFixture database) : DatabaseRoundTripTests(new MySqlHarness(database.ConnectionString));

public sealed class DuckDBRoundTripTests() : DatabaseRoundTripTests(new DuckDBHarness()), IDisposable
{
    public void Dispose() => ((DuckDBHarness)Harness).Dispose();
}
