using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using ExcelDataReader;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Excel;

/// <summary>
///     Reads a sheet of an Excel workbook (<c>.xlsx</c>, <c>.xlsm</c> or <c>.xls</c>) into records with ExcelDataReader.
///     Columns bind to members by header, case-insensitively, once per file; cell values convert strictly.
/// </summary>
/// <typeparam name="T">The record type, or a scalar type for a single-column sheet.</typeparam>
/// <remarks>
///     Workbooks are zip archives, so a stream that cannot seek (S3, SFTP, HTTP) is first copied to a temporary file.
///     The options' <see cref="FileNodeOptions.Uri" /> can also name a directory (ending in <c>/</c>) or a glob.
/// </remarks>
public sealed class ExcelSourceNode<T> : FileSourceNode<T>
{
    private readonly RecordBindingOptions _binding;
    private readonly IReadOnlyList<string> _declaredColumns;
    private readonly bool _hasHeader;
    private readonly Func<ExcelRow, T>? _map;
    private readonly ExcelReadOptions _options;

    static ExcelSourceNode()
    {
        // Legacy .xls files use code pages that .NET does not load by default.
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

    /// <summary>Creates a source that maps columns to <typeparamref name="T" />'s members by header.</summary>
    /// <exception cref="NotSupportedException">A mapped member's type cannot be read from a cell.</exception>
    public ExcelSourceNode(ExcelReadOptions options)
        : this(options, null, true)
    {
    }

    /// <summary>Creates a source that builds each record with <paramref name="map" />.</summary>
    /// <param name="options">The source's options.</param>
    /// <param name="map">Builds a record from the current row. An exception it throws is a row error.</param>
    public ExcelSourceNode(ExcelReadOptions options, Func<ExcelRow, T> map)
        : this(options, map ?? throw new ArgumentNullException(nameof(map)), false)
    {
    }

    private ExcelSourceNode(ExcelReadOptions options, Func<ExcelRow, T>? map, bool bindMembers)
        : base(options)
    {
        _options = options;
        _map = map;
        _binding = new RecordBindingOptions { Shape = new RecordShapeOptions { Naming = options.Naming }, MissingColumns = options.MissingColumns };

        var shape = RecordShape.For<T>(_binding.Shape);
        _hasHeader = options.HasHeader ?? !(bindMembers && shape.IsScalar);
        _declaredColumns = shape.Members.Select(m => m.ColumnName).ToArray();

        if (bindMembers)
            shape.ThrowIfNotFlat("Excel");
    }

    /// <inheritdoc />
    protected override string ConnectorName => "excel";

    /// <inheritdoc />
    protected override IReadOnlyList<string> DirectoryFileExtensions { get; } = [".xlsx", ".xlsm", ".xls"];

    /// <inheritdoc />
    protected override bool RequiresSeekableStream => true;

    /// <inheritdoc />
    protected override bool SupportsCompression => false;

    /// <inheritdoc />
    protected override async IAsyncEnumerable<T> ReadAsync(Stream stream, FileReadContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var reader = ExcelReaderFactory.CreateReader(stream, new ExcelReaderConfiguration { Password = _options.Password, LeaveOpen = true });
        SelectSheet(reader, context);

        var rowNumber = 0;

        for (var skipped = 0; skipped < _options.SkipRows; skipped++)
        {
            if (!reader.Read())
                yield break;

            rowNumber++;
        }

        IReadOnlyList<string> columns = _declaredColumns;

        if (_hasHeader)
        {
            if (!ReadRow(reader, ref rowNumber))
                yield break;

            columns = ReadHeader(reader);
        }

        var mapper = _map is null ? RecordBinder.Bind<T, ExcelFieldReader>(columns, _binding) : null;
        var fields = new ExcelFieldReader(reader);
        var row = _map is null ? null : new ExcelRow(reader, _hasHeader ? columns : []);
        long recordNumber = 0;

        while (ReadRow(reader, ref rowNumber))
        {
            cancellationToken.ThrowIfCancellationRequested();
            recordNumber++;
            T item = default!;
            Exception? error = null;

            try
            {
                if (mapper is not null)
                    item = mapper(fields);
                else
                {
                    row!.RecordNumber = recordNumber;
                    row.RowNumber = rowNumber;
                    item = _map!(row);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                error = ex;
            }

            if (error is not null)
            {
                await context.HandleRowErrorAsync(recordNumber, error, Describe(reader, rowNumber), cancellationToken).ConfigureAwait(false);
                continue;
            }

            yield return item;
        }
    }

    private bool ReadRow(IExcelDataReader reader, ref int rowNumber)
    {
        while (reader.Read())
        {
            rowNumber++;

            if (!_options.SkipEmptyRows || !IsEmpty(reader))
                return true;
        }

        return false;
    }

    private void SelectSheet(IExcelDataReader reader, FileReadContext context)
    {
        var index = 0;
        var names = new List<string>();

        do
        {
            names.Add(reader.Name);

            if (_options.SheetName is { } name ? string.Equals(reader.Name, name, StringComparison.OrdinalIgnoreCase) : index == _options.SheetIndex)
                return;

            index++;
        } while (reader.NextResult());

        var wanted = _options.SheetName is null ? $"sheet {_options.SheetIndex}" : $"sheet '{_options.SheetName}'";
        throw new InvalidOperationException($"'{context.Source}' has no {wanted}. Sheets: {string.Join(", ", names)}.");
    }

    private static string[] ReadHeader(IExcelDataReader reader)
    {
        var headers = new string[reader.FieldCount];

        // A header cell can be a number or a date (a "2024" column), not only text.
        for (var i = 0; i < headers.Length; i++)
        {
            headers[i] = Convert.ToString(ExcelFieldReader.Raw(reader, i), CultureInfo.InvariantCulture)?.Trim() ?? string.Empty;
        }

        return headers;
    }

    private static bool IsEmpty(IExcelDataReader reader)
    {
        for (var i = 0; i < reader.FieldCount; i++)
        {
            if (reader.GetValue(i) is { } value && (value is not string text || !string.IsNullOrWhiteSpace(text)))
                return false;
        }

        return true;
    }

    // Workbooks have no raw record, so the excerpt names the sheet and row and lists the cells.
    private static string Describe(IExcelDataReader reader, int rowNumber)
    {
        var cells = Enumerable.Range(0, reader.FieldCount).Select(i => Convert.ToString(ExcelFieldReader.Raw(reader, i), CultureInfo.InvariantCulture));
        return $"{reader.Name} row {rowNumber}: {string.Join(" | ", cells)}";
    }
}
