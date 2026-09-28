using System.Runtime.CompilerServices;
using System.Text;
using CsvHelper;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Csv;

/// <summary>
///     Reads CSV files into records. Columns bind to members by name, case-insensitively, once per file; values convert
///     strictly, so a value that does not fit its member is a row error rather than a default.
/// </summary>
/// <typeparam name="T">The record type, or a scalar type (<c>string</c>, <c>int</c>, <c>DateTime</c>…) for a single-column file.</typeparam>
/// <remarks>
///     The options' <see cref="FileNodeOptions.Uri" /> can name a file, a directory (ending in <c>/</c>, which reads its
///     <c>.csv</c> files) or a glob. Files ending in <c>.gz</c>, <c>.br</c> or <c>.zz</c> are decompressed.
/// </remarks>
public sealed class CsvSourceNode<T> : FileSourceNode<T>
{
    private static readonly Encoding DefaultEncoding = new UTF8Encoding(false);

    private readonly RecordBindingOptions _binding;
    private readonly IReadOnlyList<string> _declaredColumns;
    private readonly bool _hasHeader;
    private readonly Func<CsvRow, T>? _map;
    private readonly CsvReadOptions _options;

    /// <summary>Creates a source that maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <exception cref="NotSupportedException">A mapped member's type cannot be read from a CSV field.</exception>
    public CsvSourceNode(CsvReadOptions options)
        : this(options, null, true)
    {
    }

    /// <summary>Creates a source that builds each record with <paramref name="map" />.</summary>
    /// <param name="options">The source's options.</param>
    /// <param name="map">Builds a record from the current row. An exception it throws is a row error.</param>
    public CsvSourceNode(CsvReadOptions options, Func<CsvRow, T> map)
        : this(options, map ?? throw new ArgumentNullException(nameof(map)), false)
    {
    }

    private CsvSourceNode(CsvReadOptions options, Func<CsvRow, T>? map, bool bindMembers)
        : base(options)
    {
        _options = options;
        _map = map;
        _binding = new RecordBindingOptions { Shape = new RecordShapeOptions { Naming = options.Naming }, MissingColumns = options.MissingColumns };

        var shape = RecordShape.For<T>(_binding.Shape);

        // A manual mapper reads the file however it likes, so only member binding infers "no header" from a scalar T.
        _hasHeader = options.HasHeader ?? !(bindMembers && shape.IsScalar);
        _declaredColumns = shape.Members.Select(m => m.ColumnName).ToArray();

        if (bindMembers)
            shape.ThrowIfNotFlat("CSV");
    }

    /// <inheritdoc />
    protected override string ConnectorName => "csv";

    /// <inheritdoc />
    protected override IReadOnlyList<string> DirectoryFileExtensions { get; } = [".csv", ".csv.gz", ".csv.br", ".csv.zz"];

    /// <inheritdoc />
    protected override async IAsyncEnumerable<T> ReadAsync(Stream stream, FileReadContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        BadDataFoundArgs? badData = null;
        var configuration = CreateConfiguration(args => badData ??= args);

        using var text = new StreamReader(stream, _options.Encoding ?? DefaultEncoding, true, _options.BufferSize, true);
        using var parser = new CsvParser(text, configuration, true);

        IReadOnlyList<string> columns = _declaredColumns;

        if (_hasHeader)
        {
            if (!await parser.ReadAsync().ConfigureAwait(false))
                yield break;

            columns = parser.Record ?? [];
            badData = null;
        }

        // Bound once per file: files with the same header share one compiled mapper.
        var mapper = _map is null ? RecordBinder.Bind<T, CsvFieldReader>(columns, _binding) : null;
        var fields = new CsvFieldReader(parser, _options.Culture);
        var row = _map is null ? null : new CsvRow(parser, _hasHeader ? columns : [], _options.Culture);
        long recordNumber = 0;

        while (await parser.ReadAsync().ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            recordNumber++;
            T item = default!;
            Exception? error = null;
            badData = null;

            try
            {
                if (mapper is not null)
                    item = mapper(fields);
                else
                {
                    row!.RecordNumber = recordNumber;
                    item = _map!(row);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                error = ex;
            }

            // CsvHelper parses a field when it is read, so bad quoting surfaces during mapping, and only in fields the
            // record uses.
            if (badData is { } bad)
                error = new BadDataException(bad.Field, bad.RawRecord, bad.Context, $"Field '{bad.Field}' has a quote in an unquoted field or text after a closing quote.");

            if (error is not null)
            {
                await context.HandleRowErrorAsync(recordNumber, error, parser.RawRecord, cancellationToken).ConfigureAwait(false);
                continue;
            }

            yield return item;
        }
    }

    private CsvHelper.Configuration.CsvConfiguration CreateConfiguration(BadDataFound badDataFound)
    {
        var configuration = new CsvHelper.Configuration.CsvConfiguration(_options.Culture)
        {
            Delimiter = _options.Delimiter,
            DetectDelimiter = _options.DetectDelimiter,
            Quote = _options.Quote,
            HasHeaderRecord = _hasHeader,
            TrimOptions = _options.Trim,
            BufferSize = _options.BufferSize,
            BadDataFound = badDataFound,
        };

        _options.ConfigureCsvHelper?.Invoke(configuration);
        return configuration;
    }
}
