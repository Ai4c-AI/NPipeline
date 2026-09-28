using AwesomeAssertions;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Mapping;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.Excel.Tests;

public sealed class ExcelSourceNodeTests : ExcelTestBase
{
    [Fact]
    public async Task Maps_columns_by_header_ignoring_case_order_and_surrounding_spaces()
    {
        PutWorkbook(("Sheet1", [[" balance ", "NAME", "id"], [12.5, "Ada", 1], [7, "Grace", 2]]));

        var rows = await ReadAsync(Source<Person>());

        rows.Should().BeEquivalentTo([
            new Person { Id = 1, Name = "Ada", Balance = 12.5m },
            new Person { Id = 2, Name = "Grace", Balance = 7m },
        ]);
    }

    [Fact]
    public async Task A_missing_optional_column_keeps_the_member_initialiser()
    {
        PutWorkbook(("Sheet1", [["Id", "Name"], [1, "Ada"]]));

        (await ReadAsync(Source<Person>())).Single().Country.Should().Be("AU");
    }

    [Fact]
    public async Task Builds_positional_records()
    {
        PutWorkbook(("Sheet1", [["name", "id"], ["Ada", 1]]));

        (await ReadAsync(Source<PositionalPerson>())).Should().Equal(new PositionalPerson(1, "Ada"));
    }

    [Fact]
    public async Task Converts_cells_strictly()
    {
        PutWorkbook(("Sheet1", [["Id", "Name", "Balance"], [1.7, "Ada", 1]]));

        var read = () => ReadAsync(Source<Person>());

        var failure = (await read.Should().ThrowAsync<RecordMappingException>()).Which;
        failure.RecordNumber.Should().Be(1);
        failure.Field.Should().Be("Id");
        failure.RawExcerpt.Should().Be("Sheet1 row 2: 1.7 | Ada | 1");
    }

    [Fact]
    public async Task Parses_numbers_stored_as_text()
    {
        PutWorkbook(("Sheet1", [["Id", "Name", "Balance"], ["42", "Ada", "1234.5"]]));

        (await ReadAsync(Source<Person>())).Single().Should().BeEquivalentTo(new { Id = 42, Balance = 1234.5m });
    }

    [Fact]
    public async Task A_row_error_handler_can_skip_bad_rows()
    {
        PutWorkbook(("Sheet1", [["Id", "Name"], [1, "a"], ["x", "b"], [3, "c"]]));

        var rows = await ReadAsync(Source<Person>(o => o with { RowErrorHandler = _ => RowErrorAction.Skip }));

        rows.Select(r => r.Id).Should().Equal(1, 3);
    }

    [Fact]
    public async Task Reads_numeric_headers()
    {
        PutWorkbook(("Sheet1", [["Id", 2024], [1, 99]]));

        var rows = await ReadAsync(ExcelConnector.Source(Uri(), row => row.Get<int>("2024"), o => o with { Provider = Provider }));

        rows.Should().Equal(99);
    }

    [Fact]
    public async Task Selects_a_sheet_by_name_or_position()
    {
        PutWorkbook(("First", [["Id"], [1]]), ("Orders", [["Id"], [2]]));

        (await ReadAsync(Source<Person>(o => o with { SheetName = "orders" }))).Single().Id.Should().Be(2);
        (await ReadAsync(Source<Person>(o => o with { SheetIndex = 1 }))).Single().Id.Should().Be(2);
        (await ReadAsync(Source<Person>())).Single().Id.Should().Be(1);
    }

    [Fact]
    public async Task A_missing_sheet_fails_naming_the_sheets()
    {
        PutWorkbook(("First", [["Id"]]), ("Second", [["Id"]]));

        var read = () => ReadAsync(Source<Person>(o => o with { SheetName = "Orders" }));

        (await read.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*no sheet 'Orders'. Sheets: First, Second.");
    }

    [Fact]
    public async Task Skips_title_rows_and_empty_rows()
    {
        PutWorkbook(("Sheet1", [["Quarterly report"], [], ["Id", "Name"], [1, "a"], [null, "  "], [], [2, "b"], []]));

        var rows = await ReadAsync(Source<Person>(o => o with { SkipRows = 2 }));

        rows.Select(r => r.Id).Should().Equal(1, 2);
    }

    [Fact]
    public async Task Without_a_header_columns_follow_member_declaration_order()
    {
        PutWorkbook(("Sheet1", [[1, "Ada", 2.5, "NZ"]]));

        (await ReadAsync(Source<Person>(o => o with { HasHeader = false }))).Single()
            .Should().BeEquivalentTo(new Person { Id = 1, Name = "Ada", Balance = 2.5m, Country = "NZ" });
    }

    [Fact]
    public async Task Applies_a_naming_policy_and_attributes()
    {
        PutWorkbook(("Sheet1", [["Customer ID", "secret"], [7, "leaked"]]), ("Prefixed", [["col_name", "col_id"], ["Ada", 1]]));
        var prefixed = ColumnNamingPolicy.Custom(name => "col_" + name.ToLowerInvariant());

        (await ReadAsync(Source<Attributed>())).Single().Should().BeEquivalentTo(new { Id = 7, Secret = "unset" });
        (await ReadAsync(Source<PositionalPerson>(o => o with { SheetName = "Prefixed", Naming = prefixed }))).Should().Equal(new PositionalPerson(1, "Ada"));
    }

    [Fact]
    public async Task Reads_from_streams_that_cannot_seek()
    {
        var writer = new InMemoryStorageProvider();
        var source = new InMemoryStorageProvider { NonSeekableReads = true };
        await WriteAsync(ExcelConnector.Sink<Person>(Uri(), o => o with { Provider = writer }), new Person { Id = 1, Name = "Ada" });
        source.Put(Uri(), writer.Get(Uri()));

        var rows = await ReadAsync(ExcelConnector.Source<Person>(Uri(), o => o with { Provider = source }));

        rows.Single().Name.Should().Be("Ada");
    }

    [Fact]
    public async Task A_manual_mapper_reads_by_name_or_index()
    {
        PutWorkbook(("Sheet1", [["Id", "Name"], [1, "Ada"]]));

        var rows = await ReadAsync(ExcelConnector.Source(
            Uri(),
            row => $"{row.SheetName}:{row.RowNumber}:{row.RecordNumber}:{row.Get<int>("ID")}:{row.Get<string>(1)}:{row.TryGet<int>("Name", out _)}:{row["missing"] ?? "-"}",
            o => o with { Provider = Provider }));

        rows.Should().Equal("Sheet1:2:1:1:Ada:False:-");
    }

    [Fact]
    public void A_member_that_is_not_a_single_value_fails_when_the_node_is_created()
    {
        var create = () => Source<NestedRow>();

        create.Should().Throw<NotSupportedException>().WithMessage("*Values*");
    }

    [Fact]
    public void Validates_options()
    {
        var sheet = () => Source<Person>(o => o with { SheetIndex = -1 });
        var skip = () => Source<Person>(o => o with { SkipRows = -1 });

        sheet.Should().Throw<ArgumentOutOfRangeException>();
        skip.Should().Throw<ArgumentOutOfRangeException>();
    }
}
