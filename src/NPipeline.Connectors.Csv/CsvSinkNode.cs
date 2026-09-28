using System.Text;
using CsvHelper;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Csv;

/// <summary>
///     Writes records to a CSV file: one column per readable member, named by the options' naming policy, with values
///     formatted so that <see cref="CsvSourceNode{T}" /> reads them back unchanged (ISO 8601 dates, invariant numbers).
/// </summary>
/// <typeparam name="T">The record type, or a scalar type for a single-column file.</typeparam>
/// <remarks>
///     A target ending in <c>.gz</c>, <c>.br</c> or <c>.zz</c> is compressed. On the file system the file is written to a
///     temporary name and moved into place, so readers never see a partial file.
/// </remarks>
public sealed class CsvSinkNode<T> : FileSinkNode<T>
{
    private static readonly Encoding DefaultEncoding = new UTF8Encoding(false);

    private readonly IReadOnlyList<string> _columns;
    private readonly bool _hasHeader;
    private readonly CsvWriteOptions _options;
    private readonly Action<CsvRowWriter, T> _write;

    /// <summary>Creates a sink that writes <typeparamref name="T" />'s readable members as columns.</summary>
    /// <exception cref="NotSupportedException">A member's type cannot be written to a CSV field.</exception>
    public CsvSinkNode(CsvWriteOptions options)
        : base(options)
    {
        var shapeOptions = new RecordShapeOptions { Naming = options.Naming };
        var shape = RecordShape.For<T>(shapeOptions);
        shape.ThrowIfNotFlat("CSV");

        var plan = RecordWriterPlan.Create<T, CsvRowWriter>(shapeOptions);
        _options = options;
        _columns = plan.ColumnNames;
        _write = plan.Write;
        _hasHeader = options.HasHeader ?? !plan.IsScalar;
    }

    /// <summary>Creates a sink that writes each record with <paramref name="write" />.</summary>
    /// <param name="options">The sink's options.</param>
    /// <param name="columns">The header, written when the options call for one.</param>
    /// <param name="write">Writes one record's fields, in <paramref name="columns" /> order.</param>
    public CsvSinkNode(CsvWriteOptions options, IReadOnlyList<string> columns, Action<CsvRowWriter, T> write)
        : base(options)
    {
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(write);
        _options = options;
        _columns = columns;
        _write = write;
        _hasHeader = options.HasHeader ?? true;
    }

    /// <inheritdoc />
    protected override string ConnectorName => "csv";

    /// <inheritdoc />
    protected override async Task WriteAsync(Stream stream, IAsyncEnumerable<T> items, FileWriteContext context, CancellationToken cancellationToken)
    {
        var text = new StreamWriter(stream, _options.Encoding ?? DefaultEncoding, context.BufferSize, true);

        await using (text.ConfigureAwait(false))
        {
            var csv = new CsvWriter(text, CreateConfiguration(), true);

            await using (csv.ConfigureAwait(false))
            {
                var row = new CsvRowWriter(csv, _options.Culture);

                if (_hasHeader)
                {
                    foreach (var column in _columns)
                    {
                        csv.WriteField(column);
                    }

                    await csv.NextRecordAsync().ConfigureAwait(false);
                }

                await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    _write(row, item);
                    await csv.NextRecordAsync().ConfigureAwait(false);
                }

                // The CsvWriter's buffer first, then the StreamWriter's.
                await csv.FlushAsync().ConfigureAwait(false);
            }

            await text.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private CsvHelper.Configuration.CsvConfiguration CreateConfiguration()
    {
        var configuration = new CsvHelper.Configuration.CsvConfiguration(_options.Culture)
        {
            Delimiter = _options.Delimiter,
            Quote = _options.Quote,
            HasHeaderRecord = _hasHeader,
            NewLine = _options.NewLine,
            BufferSize = _options.BufferSize,
        };

        _options.ConfigureCsvHelper?.Invoke(configuration);
        return configuration;
    }
}

/// <summary>
///     Writes the fields of one CSV row, in order. A manual writer passed to <see cref="CsvSinkNode{T}" /> calls
///     <see cref="Write{TValue}" /> once per column.
/// </summary>
public sealed class CsvRowWriter : IFieldWriter
{
    private readonly IFormatProvider _culture;
    private readonly CsvWriter _writer;

    internal CsvRowWriter(CsvWriter writer, IFormatProvider culture)
    {
        _writer = writer;
        _culture = culture;
    }

    /// <summary>Writes the next field, formatted as <see cref="ScalarFormatter" /> formats it; <c>null</c> is an empty field.</summary>
    /// <exception cref="NotSupportedException"><typeparamref name="TValue" /> is not a scalar type.</exception>
    public void Write<TValue>(TValue value) => _writer.WriteField(ScalarFormatter.Format(value, _culture));

    /// <summary>Writes the next field as text, quoted when it needs to be.</summary>
    public void WriteText(string? text) => _writer.WriteField(text);

    /// <inheritdoc />
    void IFieldWriter.WriteValue<TValue>(int ordinal, TValue value) => Write(value);
}
