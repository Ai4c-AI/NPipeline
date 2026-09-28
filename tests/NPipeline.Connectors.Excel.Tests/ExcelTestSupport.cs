using System.Globalization;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using NPipeline.Connectors.Attributes;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.Excel.Tests;

/// <summary>An in-memory store and helpers to run Excel nodes against it.</summary>
public abstract class ExcelTestBase
{
    protected InMemoryStorageProvider Provider { get; init; } = new();

    protected static StorageUri Uri(string path = "data.xlsx") => InMemoryStorageProvider.Uri(path);

    protected ExcelSourceNode<T> Source<T>(Func<ExcelReadOptions, ExcelReadOptions>? configure = null, string path = "data.xlsx") =>
        ExcelConnector.Source<T>(Uri(path), o => (configure ?? (x => x))(o with { Provider = Provider }));

    protected ExcelSinkNode<T> Sink<T>(Func<ExcelWriteOptions, ExcelWriteOptions>? configure = null, string path = "data.xlsx") =>
        ExcelConnector.Sink<T>(Uri(path), o => (configure ?? (x => x))(o with { Provider = Provider }));

    protected SpreadsheetDocument Open(string path = "data.xlsx") => SpreadsheetDocument.Open(new MemoryStream(Provider.Get(Uri(path))), false);

    /// <summary>Builds a workbook with the OpenXML SDK, as another tool would: one sheet per entry, cells from strings, numbers, booleans and nulls.</summary>
    protected void PutWorkbook(params (string Name, object?[][] Rows)[] sheets) => Provider.Put(Uri(), BuildWorkbook(sheets));

    protected static async Task<List<T>> ReadAsync<T>(SourceNode<T> source)
    {
        var rows = new List<T>();

        await foreach (var row in source.OpenStream(new PipelineContext(), CancellationToken.None))
        {
            rows.Add(row);
        }

        return rows;
    }

    protected static async Task WriteAsync<T>(SinkNode<T> sink, params T[] items)
    {
        await using var input = new DataStream<T>(Enumerate(items), "items");
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
    }

    private static async IAsyncEnumerable<T> Enumerate<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }

    private static byte[] BuildWorkbook((string Name, object?[][] Rows)[] sheets)
    {
        using var buffer = new MemoryStream();

        using (var document = SpreadsheetDocument.Create(buffer, SpreadsheetDocumentType.Workbook))
        {
            var workbookPart = document.AddWorkbookPart();
            workbookPart.Workbook = new Workbook(new Sheets());
            uint id = 1;

            foreach (var (name, rows) in sheets)
            {
                var worksheetPart = workbookPart.AddNewPart<WorksheetPart>();
                worksheetPart.Worksheet = new Worksheet(new SheetData(rows.Select(cells => new Row(cells.Select(ToCell).Cast<OpenXmlElement>()))));
                workbookPart.Workbook.Sheets!.AppendChild(new Sheet { Id = workbookPart.GetIdOfPart(worksheetPart), SheetId = id++, Name = name });
            }

            workbookPart.Workbook.Save();
        }

        return buffer.ToArray();
    }

    private static Cell ToCell(object? value) =>
        value switch
        {
            null => new Cell(),
            string text => new Cell { DataType = CellValues.InlineString, InlineString = new InlineString(new Text(text)) },
            bool flag => new Cell { DataType = CellValues.Boolean, CellValue = new CellValue(flag ? "1" : "0") },
            IFormattable number => new Cell { DataType = CellValues.Number, CellValue = new CellValue(number.ToString(null, CultureInfo.InvariantCulture)) },
            _ => throw new ArgumentException($"Unsupported test cell value {value}."),
        };
}

public enum Tier
{
    Free,
    Pro,
}

public sealed class Person
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public decimal Balance { get; set; }

    public string Country { get; set; } = "AU";
}

public sealed record PositionalPerson(int Id, string Name);

public sealed class TypedRow
{
    public long Big { get; set; }

    public double Ratio { get; set; }

    public decimal Precise { get; set; }

    public bool Flag { get; set; }

    public DateTime At { get; set; }

    public DateTimeOffset Offset { get; set; }

    public DateOnly Day { get; set; }

    public TimeOnly Time { get; set; }

    public TimeSpan Span { get; set; }

    public Guid Key { get; set; }

    public Tier Tier { get; set; }

    public int? Missing { get; set; }
}

public sealed class Attributed
{
    [Column("Customer ID")]
    public int Id { get; set; }

    [IgnoreColumn]
    public string Secret { get; set; } = "unset";
}

public sealed class NestedRow
{
    public int Id { get; set; }

    public List<int> Values { get; set; } = [];
}
