using System.Text;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using NPipeline.Connectors.Excel;
using NPipeline.Connectors.RoundTrip.Tests.Harnesses;
using NPipeline.Connectors.RoundTrip.Tests.Infrastructure;
using NPipeline.Connectors.RoundTrip.Tests.Models;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.RoundTrip.Tests;

public sealed class ExcelRoundTripTests
{
    private readonly ExcelHarness _harness = new();

    [Fact]
    public Task Scalars() => RoundTripScenarios.Scalars(_harness);

    [Fact]
    public Task SmallAndUnsignedIntegers() => RoundTripScenarios.SmallAndUnsignedIntegers(_harness);

    [Fact]
    public Task Nullables() => RoundTripScenarios.Nullables(_harness);

    [Fact]
    public Task Enums() => RoundTripScenarios.Enums(_harness);

    [Fact]
    public Task DateTimeOffsets() => RoundTripScenarios.DateTimeOffsets(_harness);

    [Fact]
    public Task DateOnlys() => RoundTripScenarios.DateOnlys(_harness);

    [Fact]
    public Task Text() => RoundTripScenarios.Text(_harness);

    [Fact]
    public Task ControlCharacters() => RoundTripScenarios.ControlCharacters(_harness);

    [Fact]
    public Task PositionalRecords() => RoundTripScenarios.PositionalRecords(_harness);

    [Fact]
    public Task Volume() => RoundTripScenarios.Volume(_harness);

    [Fact]
    public Task Empty() => RoundTripScenarios.Empty(_harness);

    [Fact]
    public Task Reads_from_non_seekable_streams() =>
        RoundTripScenarios.Scalars(new ExcelHarness { Provider = new InMemoryStorageProvider { NonSeekableReads = true } });

    [Fact]
    public Task Writes_to_non_seekable_streams() =>
        RoundTripScenarios.Scalars(new ExcelHarness { Provider = new InMemoryStorageProvider { NonSeekableWrites = true } });

    [Fact]
    public async Task Source_surfaces_row_mapper_exceptions()
    {
        await _harness.WriteAsync([ScalarRecord.Create(1)]);
        var source = ExcelConnector.Source<int>(_harness.Uri, _ => throw new InvalidOperationException("mapper failed"), o => o with { Provider = _harness.Provider });

        var read = () => NodeRunner.ReadAsync(source);

        await read.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public async Task Source_rejects_unparseable_values_instead_of_defaulting()
    {
        await _harness.WriteAsync([new TextRecord { Id = 1, Text = "not-a-number" }]);
        var source = ExcelConnector.Source(_harness.Uri, row => row.Get<int>("text"), o => o with { Provider = _harness.Provider });

        var read = () => NodeRunner.ReadAsync(source);

        await read.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public async Task Missing_columns_keep_property_initialisers()
    {
        await _harness.WriteAsync([new IdOnlyRecord { Id = 1 }]);

        var rows = await _harness.ReadAsync<DefaultedRecord>();

        rows.Should().ContainSingle().Which.Country.Should().Be("AU");
    }

    [Fact]
    public async Task Source_reads_numeric_header_cells()
    {
        _harness.Provider.Put(_harness.Uri, BuildWorkbook(
            [Inline("Id"), Number("2024")],
            [Number("1"), Number("99")]));

        var source = ExcelConnector.Source(_harness.Uri, row => (row.Get<int>("Id"), row.Get<int>("2024")), o => o with { Provider = _harness.Provider });
        var rows = await NodeRunner.ReadAsync(source);

        rows.Should().Equal((1, 99));
    }

    [Fact]
    public async Task Sink_styles_date_cells_as_dates()
    {
        await _harness.WriteAsync([ScalarRecord.Create(1)]);

        using var document = SpreadsheetDocument.Open(new MemoryStream(_harness.Provider.Get(_harness.Uri)), false);

        document.WorkbookPart!.WorkbookStylesPart.Should().NotBeNull("date cells need a date number format to display as dates");
    }

    [Fact]
    public async Task Sink_preserves_leading_and_trailing_whitespace_for_excel()
    {
        await _harness.WriteAsync([new TextRecord { Id = 1, Text = "  padded  " }]);

        using var document = SpreadsheetDocument.Open(new MemoryStream(_harness.Provider.Get(_harness.Uri)), false);
        var text = document.WorkbookPart!.WorksheetParts.Single().Worksheet!.Descendants<Text>().Single(t => t.Text == "  padded  ");

        text.Space.Should().NotBeNull("Excel trims inline strings without xml:space=\"preserve\"");
        text.Space!.Value.Should().Be(SpaceProcessingModeValues.Preserve, "Excel trims inline strings without xml:space=\"preserve\"");
    }

    private static Cell Inline(string text) => new() { DataType = CellValues.InlineString, InlineString = new InlineString(new Text(text)) };

    private static Cell Number(string value) => new() { DataType = CellValues.Number, CellValue = new CellValue(value) };

    private static byte[] BuildWorkbook(params Cell[][] rows)
    {
        using var buffer = new MemoryStream();

        using (var document = SpreadsheetDocument.Create(buffer, SpreadsheetDocumentType.Workbook))
        {
            var workbookPart = document.AddWorkbookPart();
            workbookPart.Workbook = new Workbook();
            var worksheetPart = workbookPart.AddNewPart<WorksheetPart>();
            worksheetPart.Worksheet = new Worksheet(new SheetData(rows.Select(cells => new Row(cells.Cast<OpenXmlElement>()))));
            workbookPart.Workbook.AppendChild(new Sheets(new Sheet { Id = workbookPart.GetIdOfPart(worksheetPart), SheetId = 1, Name = "Sheet1" }));
            workbookPart.Workbook.Save();
        }

        return buffer.ToArray();
    }

    public sealed class IdOnlyRecord
    {
        public int Id { get; set; }
    }

    public sealed class DefaultedRecord
    {
        public int Id { get; set; }

        public string Country { get; set; } = "AU";
    }
}
