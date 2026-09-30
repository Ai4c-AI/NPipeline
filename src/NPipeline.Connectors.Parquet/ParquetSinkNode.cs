using System.Collections.Concurrent;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Parquet.Schema;
using NPipeline.Connectors.Parquet.Writing;
using Parquet;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet;

/// <summary>
///     Writes records to a Parquet file. Each record's members are written straight into typed column buffers, and a row
///     group is flushed at <see cref="ParquetWriteOptions.RowGroupSize" /> rows or <see cref="ParquetWriteOptions.RowGroupBytes" />,
///     whichever comes first.
/// </summary>
/// <typeparam name="T">The record type, or a scalar type for a one-column file.</typeparam>
/// <remarks>
///     Types map to columns as the connector documentation lists: dates as UTC timestamps, <see cref="Guid" /> as
///     <c>UUID</c>, enums as their names, <c>decimal</c> as <c>DECIMAL(38, 18)</c> unless
///     <see cref="Attributes.ParquetDecimalAttribute" /> says otherwise, and lists as <c>LIST</c> columns. On the file
///     system the file is written under a temporary name and moved into place; object stores are written directly.
/// </remarks>
public sealed class ParquetSinkNode<T> : FileSinkNode<T>
{
    private readonly ParquetWriteOptions _options;
    private readonly ParquetWriteLayout<T> _layout;

    /// <summary>Creates a sink that writes <typeparamref name="T" />'s readable members as columns.</summary>
    /// <exception cref="NotSupportedException">A member's type has no Parquet column.</exception>
    public ParquetSinkNode(ParquetWriteOptions options)
        : base(options)
    {
        _options = options;
        _layout = ParquetWriteLayout<T>.For(options.Naming);
    }

    /// <summary>The schema the sink writes.</summary>
    public ParquetSchema Schema => _layout.Schema;

    /// <inheritdoc />
    protected override string ConnectorName => "parquet";

    /// <inheritdoc />
    protected override bool SupportsCompression => false;

    /// <inheritdoc />
    protected override async Task WriteAsync(Stream stream, IAsyncEnumerable<T> items, FileWriteContext context, CancellationToken cancellationToken)
    {
        var builders = _layout.CreateBuilders();

        try
        {
            var writer = await ParquetWriter.CreateAsync(_layout.Schema, stream, new ParquetOptions { CompressionMethod = _options.Codec }, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            await using (writer.ConfigureAwait(false))
            {
                var fields = new ParquetFieldWriter(builders);
                var rows = 0;

                await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    _layout.Write(fields, item);
                    rows++;

                    // Sizes are summed every 256 rows: often enough for row groups to land near the target.
                    if (rows >= _options.RowGroupSize || ((rows & 255) == 0 && EstimatedBytes(builders) >= _options.RowGroupBytes))
                    {
                        await FlushAsync(writer, builders, cancellationToken).ConfigureAwait(false);
                        rows = 0;
                    }
                }

                if (rows > 0)
                    await FlushAsync(writer, builders, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            foreach (var builder in builders)
            {
                builder.Dispose();
            }
        }
    }

    private static long EstimatedBytes(ParquetColumnBuilder[] builders)
    {
        long total = 0;

        foreach (var builder in builders)
        {
            total += builder.EstimatedBytes;
        }

        return total;
    }

    private static async Task FlushAsync(ParquetWriter writer, ParquetColumnBuilder[] builders, CancellationToken cancellationToken)
    {
        using var rowGroup = writer.CreateRowGroup();

        foreach (var builder in builders)
        {
            await builder.WriteAsync(rowGroup, cancellationToken).ConfigureAwait(false);
            builder.Reset();
        }
    }
}

/// <summary>The schema and compiled writer for a record type, built once per type and naming policy.</summary>
internal sealed class ParquetWriteLayout<T>
{
    private static readonly ConcurrentDictionary<ColumnNamingPolicy, ParquetWriteLayout<T>> Cache = new();

    private readonly Type[] _memberTypes;
    private readonly RecordWriterPlan<T, ParquetFieldWriter> _plan;

    private ParquetWriteLayout(ColumnNamingPolicy naming)
    {
        var shapeOptions = ParquetShape.Options(naming);
        var shape = RecordShape.For<T>(shapeOptions);
        ParquetShape.ThrowIfUnsupported(shape);

        _plan = RecordWriterPlan.Create<T, ParquetFieldWriter>(shapeOptions);

        if (_plan.IsScalar)
        {
            if (!ParquetTypeMap.IsSupported(typeof(T)))
                throw new NotSupportedException($"{typeof(T).Name} has no Parquet column type.");

            _memberTypes = [typeof(T)];
            Schema = new ParquetSchema(ParquetTypeMap.CreateField(_plan.ColumnNames[0], typeof(T), null));
        }
        else
        {
            var members = shape.Members.Where(m => m.CanRead).ToList();
            _memberTypes = [.. members.Select(m => m.Type)];
            Schema = new ParquetSchema(members.Select(m => ParquetTypeMap.CreateField(m.ColumnName, m.Type, m.Member)).ToArray());
        }
    }

    public ParquetSchema Schema { get; }

    public static ParquetWriteLayout<T> For(ColumnNamingPolicy naming) => Cache.GetOrAdd(naming, static policy => new ParquetWriteLayout<T>(policy));

    public ParquetColumnBuilder[] CreateBuilders() =>
        [.. Schema.Fields.Select((field, i) => ParquetColumnBuilders.ForMember(field, _memberTypes[i]))];

    public void Write(ParquetFieldWriter fields, T item) => _plan.Write(fields, item);
}

/// <summary>The row the compiled writer writes: each member into its column's builder, with its static type.</summary>
internal sealed class ParquetFieldWriter(ParquetColumnBuilder[] builders) : IFieldWriter
{
    public void WriteValue<TValue>(int ordinal, TValue value) => ((ParquetColumnBuilder<TValue>)builders[ordinal]).Add(value);
}
