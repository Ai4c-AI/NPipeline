using System.Globalization;
using System.Runtime.CompilerServices;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Parquet.Reading;
using NPipeline.Connectors.Parquet.Schema;
using NPipeline.StorageProviders.Models;
using Parquet;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet;

/// <summary>
///     Reads Parquet files into records. Each row group's mapped columns are read into typed arrays and records are built
///     from them by a mapper compiled once per file layout, so values are neither boxed nor looked up by name; columns the
///     record does not map are not read at all.
/// </summary>
/// <typeparam name="T">The record type, or <see cref="ParquetRow" /> with a manual mapper.</typeparam>
/// <remarks>
///     The options' <see cref="FileNodeOptions.Uri" /> can name a file, a directory (ending in <c>/</c>, which reads its
///     <c>.parquet</c> files) or a glob. Parquet needs to seek, so a stream that cannot (S3, SFTP) is first copied to a
///     temporary file.
/// </remarks>
public sealed class ParquetSourceNode<T> : FileSourceNode<T>
{
    private readonly RecordBindingOptions _binding;
    private readonly Func<ParquetRow, T>? _map;
    private readonly HashSet<string> _mappedColumns;
    private readonly ParquetReadOptions _options;
    private readonly bool _scalar;

    /// <summary>Creates a source that maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <exception cref="NotSupportedException">A mapped member's type has no Parquet column.</exception>
    public ParquetSourceNode(ParquetReadOptions options)
        : this(options, null, true)
    {
    }

    /// <summary>Creates a source that builds each record from a <see cref="ParquetRow" /> with <paramref name="map" />.</summary>
    /// <param name="options">The source's options.</param>
    /// <param name="map">Builds a record from a row. An exception it throws is a row error.</param>
    public ParquetSourceNode(ParquetReadOptions options, Func<ParquetRow, T> map)
        : this(options, map ?? throw new ArgumentNullException(nameof(map)), false)
    {
    }

    private ParquetSourceNode(ParquetReadOptions options, Func<ParquetRow, T>? map, bool bindMembers)
        : base(options)
    {
        _options = options;
        _map = map;
        _binding = new RecordBindingOptions { Shape = ParquetShape.Options(options.Naming), MissingColumns = options.MissingColumns };
        _mappedColumns = new HashSet<string>(options.ProjectedColumns ?? [], StringComparer.OrdinalIgnoreCase);

        if (bindMembers)
        {
            var shape = RecordShape.For<T>(_binding.Shape);
            ParquetShape.ThrowIfUnsupported(shape);
            _mappedColumns.UnionWith(shape.Members.Select(m => m.ColumnName));

            // A scalar record is the file's first column, whatever its name.
            _scalar = shape.IsScalar;
        }
    }

    /// <inheritdoc />
    protected override string ConnectorName => "parquet";

    /// <inheritdoc />
    protected override IReadOnlyList<string> DirectoryFileExtensions { get; } = [".parquet"];

    /// <inheritdoc />
    protected override bool RequiresSeekableStream => true;

    /// <inheritdoc />
    protected override bool SupportsCompression => false;

    /// <inheritdoc />
    protected override async IAsyncEnumerable<T> ReadAsync(Stream stream, FileReadContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var reader = await ParquetReader.CreateAsync(stream, leaveStreamOpen: true, cancellationToken: cancellationToken).ConfigureAwait(false);

        await using (reader.ConfigureAwait(false))
        {
            if (_options.SchemaValidator is { } validate && !validate(reader.Schema))
                throw new ParquetSchemaException($"The schema of '{context.Source}' was rejected by SchemaValidator.");

            var fields = reader.Schema.Fields;
            var columns = fields.Select(ParquetColumn.Create).ToArray();
            var names = fields.Select(f => f.Name).ToList();
            var partitions = _options.PartitionColumns ? PartitionValues(context.Uri, names) : [];
            var columnNames = names.Concat(partitions.Select(p => p.Key)).ToArray();

            // Only the columns the record maps (or the options name) are read; a manual mapper without projection reads all.
            var readAll = _map is not null && _options.ProjectedColumns is null;

            var loaded = Enumerable.Range(0, columns.Length)
                .Where(i => columns[i] is not null && (readAll || (_scalar && i == 0) || _mappedColumns.Contains(names[i])))
                .ToArray();

            var mapper = _map is null ? RecordBinder.Bind<T, ParquetFieldReader>(columnNames, _binding) : null;
            var fieldReader = new ParquetFieldReader(columns, names, [.. partitions.Select(p => p.Value)]);
            var layout = new ParquetRowLayout(reader.Schema, [.. loaded.Select(i => names[i])]);
            var statistics = RowGroupFields(fields);
            var millisecondTimestamps = reader.Metadata?.CreatedBy?.StartsWith("Parquet.Net", StringComparison.Ordinal) == true;
            long recordNumber = 0;

            for (var group = 0; group < reader.RowGroupCount; group++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                using var rowGroup = reader.OpenRowGroupReader(group);

                if (_options.RowGroupFilter is { } keep && !keep(new ParquetRowGroupInfo(context.Source, group, rowGroup, statistics, millisecondTimestamps)))
                {
                    recordNumber += rowGroup.RowCount;
                    continue;
                }

                foreach (var ordinal in loaded)
                {
                    await columns[ordinal]!.LoadAsync(rowGroup, cancellationToken).ConfigureAwait(false);
                }

                var rowCount = checked((int)rowGroup.RowCount);

                for (var row = 0; row < rowCount; row++)
                {
                    recordNumber++;
                    fieldReader.Row = row;
                    T item = default!;
                    Exception? error = null;
                    ParquetRow? snapshot = null;

                    try
                    {
                        if (_options.RowFilter is { } filter && !filter(snapshot = Snapshot(layout, columns, loaded, row, recordNumber)))
                            continue;

                        item = mapper is not null ? mapper(fieldReader) : _map!(snapshot ?? Snapshot(layout, columns, loaded, row, recordNumber));
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        error = ex;
                    }

                    if (error is not null)
                    {
                        await context.HandleRowErrorAsync(recordNumber, error, Describe(columns, loaded, names, row), cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    yield return item;
                }
            }
        }
    }

    private static ParquetRow Snapshot(ParquetRowLayout layout, ParquetColumn?[] columns, int[] loaded, int row, long recordNumber)
    {
        var values = new object?[loaded.Length];

        for (var i = 0; i < loaded.Length; i++)
        {
            values[i] = columns[loaded[i]]!.Boxed(row);
        }

        return new ParquetRow(layout, values, recordNumber);
    }

    // A file has no raw record text, so the excerpt lists the row's values.
    private static string Describe(ParquetColumn?[] columns, int[] loaded, List<string> names, int row) =>
        string.Join(", ", loaded.Select(i => $"{names[i]}={Convert.ToString(columns[i]!.Boxed(row), CultureInfo.InvariantCulture)}"));

    private static Dictionary<string, DataField> RowGroupFields(IReadOnlyList<Field> fields)
    {
        var map = new Dictionary<string, DataField>(StringComparer.OrdinalIgnoreCase);

        foreach (var field in fields.OfType<DataField>())
        {
            _ = map.TryAdd(field.Name, field);
        }

        return map;
    }

    /// <summary>The <c>key=value</c> directories in the file's path, for keys the file has no column for.</summary>
    private static List<KeyValuePair<string, string?>> PartitionValues(StorageUri file, List<string> columns)
    {
        var result = new List<KeyValuePair<string, string?>>();
        var segments = file.Path.Split('/', StringSplitOptions.RemoveEmptyEntries);

        foreach (var segment in segments.Take(segments.Length - 1))
        {
            var separator = segment.IndexOf('=', StringComparison.Ordinal);

            if (separator <= 0)
                continue;

            var key = Uri.UnescapeDataString(segment[..separator]);

            if (columns.Contains(key, StringComparer.OrdinalIgnoreCase) || result.Any(p => string.Equals(p.Key, key, StringComparison.OrdinalIgnoreCase)))
                continue;

            // Hive writes a null partition value as __HIVE_DEFAULT_PARTITION__.
            var value = Uri.UnescapeDataString(segment[(separator + 1)..]);
            result.Add(new KeyValuePair<string, string?>(key, value == "__HIVE_DEFAULT_PARTITION__" ? null : value));
        }

        return result;
    }
}
