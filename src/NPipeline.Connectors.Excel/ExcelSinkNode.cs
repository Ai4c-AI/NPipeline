using System.IO.Compression;
using System.Xml;
using NPipeline.Connectors.Excel.Xlsx;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Excel;

/// <summary>
///     Writes records to one sheet of an XLSX workbook. The workbook is streamed: rows go to storage as they are written,
///     so memory stays constant however many rows there are, and non-seekable streams (S3, Azure) need no buffer.
/// </summary>
/// <typeparam name="T">The record type, or a scalar type for a single-column sheet.</typeparam>
/// <remarks>
///     Each readable member becomes a column. Numbers, booleans and dates are typed cells, and dates carry a date format
///     so Excel shows them as dates; see <see cref="ExcelRowWriter" /> for how each type is written. A sheet holds at
///     most 1,048,576 rows and 16,384 columns, and a cell at most 32,767 characters; the write fails beyond them.
/// </remarks>
public sealed class ExcelSinkNode<T> : FileSinkNode<T>
{
    private const int DrainThresholdBytes = 64 * 1024;

    private readonly IReadOnlyList<string> _columns;
    private readonly bool _hasHeader;
    private readonly ExcelWriteOptions _options;
    private readonly Action<ExcelRowWriter, T> _write;

    /// <summary>Creates a sink that writes <typeparamref name="T" />'s readable members as columns.</summary>
    /// <exception cref="NotSupportedException">A member's type cannot be written to a cell.</exception>
    public ExcelSinkNode(ExcelWriteOptions options)
        : base(options)
    {
        var shapeOptions = new RecordShapeOptions { Naming = options.Naming };
        RecordShape.For<T>(shapeOptions).ThrowIfNotFlat("Excel");

        var plan = RecordWriterPlan.Create<T, ExcelRowWriter>(shapeOptions);
        _options = options;
        _columns = CheckColumns(plan.ColumnNames);
        _write = plan.Write;
        _hasHeader = options.HasHeader ?? !plan.IsScalar;
    }

    /// <summary>Creates a sink that writes each record with <paramref name="write" />.</summary>
    /// <param name="options">The sink's options.</param>
    /// <param name="columns">The header, written when the options call for one.</param>
    /// <param name="write">Writes one record's cells, in <paramref name="columns" /> order.</param>
    public ExcelSinkNode(ExcelWriteOptions options, IReadOnlyList<string> columns, Action<ExcelRowWriter, T> write)
        : base(options)
    {
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(write);
        _options = options;
        _columns = CheckColumns(columns);
        _write = write;
        _hasHeader = options.HasHeader ?? true;
    }

    /// <inheritdoc />
    protected override string ConnectorName => "excel";

    /// <inheritdoc />
    protected override bool SupportsCompression => false;

    /// <inheritdoc />
    protected override async Task WriteAsync(Stream stream, IAsyncEnumerable<T> items, FileWriteContext context, CancellationToken cancellationToken)
    {
        var output = new ChunkedWriteStream();

        await using (output.ConfigureAwait(false))
        {
            string? filterRange = null;

            using (var archive = new ZipArchive(output, ZipArchiveMode.Create, true))
            {
                XlsxPackage.WriteLeadingParts(archive);

                var entry = archive.CreateEntry(XlsxPackage.SheetPath, CompressionLevel.Fastest);

                using (var entryStream = entry.Open())
                using (var xml = XmlWriter.Create(entryStream, XlsxSheetWriter.Settings))
                {
                    var sheet = new XlsxSheetWriter(xml, _columns.Count);
                    var row = new ExcelRowWriter(sheet);
                    sheet.BeginSheet(_options.FreezeHeader && _hasHeader);

                    if (_hasHeader)
                    {
                        sheet.BeginRow();

                        foreach (var column in _columns)
                        {
                            sheet.WriteText(column, _options.BoldHeader ? XlsxPackage.HeaderStyle : 0);
                        }

                        sheet.EndRow();
                    }

                    await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
                    {
                        sheet.BeginRow();
                        _write(row, item);
                        sheet.EndRow();

                        if (output.Pending >= DrainThresholdBytes)
                            await output.DrainAsync(stream, cancellationToken).ConfigureAwait(false);
                    }

                    if (_options.AutoFilter && _hasHeader)
                        filterRange = sheet.Range(_columns.Count);

                    sheet.EndSheet(filterRange);
                }

                XlsxPackage.WriteWorkbook(archive, _options.SheetName, filterRange);
            }

            // Disposing the archive wrote its central directory.
            await output.DrainAsync(stream, cancellationToken).ConfigureAwait(false);
        }
    }

    private static IReadOnlyList<string> CheckColumns(IReadOnlyList<string> columns) =>
        columns.Count <= XlsxSheetWriter.MaxColumns
            ? columns
            : throw new NotSupportedException($"An Excel sheet holds at most {XlsxSheetWriter.MaxColumns:N0} columns, but the record has {columns.Count:N0}.");
}
