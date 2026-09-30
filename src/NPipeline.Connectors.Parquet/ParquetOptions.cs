using System.Collections.Concurrent;
using System.Reflection;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.StorageProviders.Models;
using Parquet;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet;

/// <summary>Options for <see cref="ParquetSourceNode{T}" />. Create them with <see cref="ParquetConnector.Source{T}(StorageUri, Func{ParquetReadOptions, ParquetReadOptions}?)" /> or directly.</summary>
public sealed record ParquetReadOptions : FileSourceOptions
{
    /// <summary>
    ///     Columns to read as well as the ones the record maps. With attribute mapping, only mapped columns are read by
    ///     default; with a manual mapper, every column is read unless this is set, and then only these.
    /// </summary>
    public IReadOnlyList<string>? ProjectedColumns { get; init; }

    /// <summary>Checks each file's schema before it is read; returning <c>false</c> fails the read with a <see cref="ParquetSchemaException" />.</summary>
    public Func<ParquetSchema, bool>? SchemaValidator { get; init; }

    /// <summary>
    ///     Decides whether to read each row group, from its row count and column ranges, so whole row groups can be skipped
    ///     without reading their columns: <c>g =&gt; !g.TryGetRange&lt;DateTime&gt;("At", out _, out var max) || max &gt;= since</c>.
    /// </summary>
    public Func<ParquetRowGroupInfo, bool>? RowGroupFilter { get; init; }

    /// <summary>
    ///     Decides whether to emit each row. The row holds the columns read (see <see cref="ProjectedColumns" />). Filtering
    ///     builds a <see cref="ParquetRow" /> per row, so prefer <see cref="RowGroupFilter" /> or a later filter node when
    ///     speed matters.
    /// </summary>
    public Func<ParquetRow, bool>? RowFilter { get; init; }

    /// <summary>
    ///     Whether <c>key=value</c> directory names in a file's path (Hive-style partitions, <c>year=2026/month=09/</c>) supply
    ///     values for members of the same name that the file has no column for. Defaults to <c>true</c>.
    /// </summary>
    public bool PartitionColumns { get; init; } = true;

    /// <summary>How member names become column names. Defaults to <see cref="ColumnNamingPolicy.AsIs" />; columns match case-insensitively.</summary>
    public ColumnNamingPolicy Naming { get; init; } = ColumnNamingPolicy.AsIs;

    /// <summary>What to do when a mapped member has no column. Defaults to <see cref="MissingColumnBehavior.ThrowForRequired" />.</summary>
    public MissingColumnBehavior MissingColumns { get; init; } = MissingColumnBehavior.ThrowForRequired;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentNullException.ThrowIfNull(Naming, nameof(Naming));
    }
}

/// <summary>Options for <see cref="ParquetSinkNode{T}" />. Create them with <see cref="ParquetConnector.Sink{T}(StorageUri, Func{ParquetWriteOptions, ParquetWriteOptions}?)" /> or directly.</summary>
public sealed record ParquetWriteOptions : FileSinkOptions
{
    /// <summary>The default most rows per row group, 50,000.</summary>
    public const int DefaultRowGroupSize = 50_000;

    /// <summary>The default most estimated bytes per row group, 128 MB.</summary>
    public const long DefaultRowGroupBytes = 128L * 1024 * 1024;

    /// <summary>The most rows per row group. Defaults to 50,000.</summary>
    public int RowGroupSize { get; init; } = DefaultRowGroupSize;

    /// <summary>
    ///     The most estimated bytes of values per row group, before compression. A row group ends at whichever of this and
    ///     <see cref="RowGroupSize" /> comes first, so wide rows do not make huge row groups. Defaults to 128 MB.
    /// </summary>
    public long RowGroupBytes { get; init; } = DefaultRowGroupBytes;

    /// <summary>The compression codec for column data. Defaults to <see cref="CompressionMethod.Snappy" />.</summary>
    public CompressionMethod Codec { get; init; } = CompressionMethod.Snappy;

    /// <summary>How member names become column names. Defaults to <see cref="ColumnNamingPolicy.AsIs" />.</summary>
    public ColumnNamingPolicy Naming { get; init; } = ColumnNamingPolicy.AsIs;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(RowGroupSize, nameof(RowGroupSize));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(RowGroupBytes, nameof(RowGroupBytes));
        ArgumentNullException.ThrowIfNull(Naming, nameof(Naming));
    }
}

/// <summary>How records map to Parquet columns: the shared shape, plus <see cref="ParquetColumnAttribute" />.</summary>
internal static class ParquetShape
{
    private static readonly ConcurrentDictionary<ColumnNamingPolicy, RecordShapeOptions> Cache = new();

    // Static delegates, so options built for the same policy are equal and share the binder's cache.
    private static readonly Func<MemberInfo, string?> ColumnName = static member => member.GetCustomAttribute<ParquetColumnAttribute>(true)?.Name;

    private static readonly Func<MemberInfo, bool> IsIgnored = static member => member.GetCustomAttribute<ParquetColumnAttribute>(true)?.Ignore == true;

    public static RecordShapeOptions Options(ColumnNamingPolicy naming) =>
        Cache.GetOrAdd(naming, static policy => new RecordShapeOptions { Naming = policy, ColumnName = ColumnName, IsIgnored = IsIgnored });

    /// <summary>Throws when a mapped member's type has no Parquet column, naming every such member.</summary>
    public static void ThrowIfUnsupported(RecordShape shape)
    {
        var unsupported = shape.Members.Where(m => !Schema.ParquetTypeMap.IsSupported(m.Type)).Select(m => $"{m.Name} ({m.Type.Name})").ToList();

        if (unsupported.Count > 0)
        {
            throw new NotSupportedException(
                $"Parquet columns hold single values or lists of them, but {shape.Type.Name} maps {string.Join(", ", unsupported)}. " +
                "Mark those members [IgnoreColumn], or flatten them.");
        }
    }
}
