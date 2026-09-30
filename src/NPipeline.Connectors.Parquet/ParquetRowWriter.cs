using NPipeline.Connectors.Parquet.Writing;
using Parquet;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet;

/// <summary>
///     Writes <see cref="ParquetRow" />s back to a Parquet file with the schema they were read with, for code that moves rows
///     whose type is not known in advance, such as compaction.
/// </summary>
public static class ParquetRowWriter
{
    /// <summary>Writes <paramref name="rows" /> to <paramref name="stream" />, in row groups of at most <paramref name="rowGroupSize" /> rows.</summary>
    /// <param name="stream">Where to write. It is left open.</param>
    /// <param name="schema">The schema to write; every top-level column must be a value or a list of values. A row without one of its columns writes null.</param>
    /// <param name="rows">The rows.</param>
    /// <param name="codec">The compression codec.</param>
    /// <param name="rowGroupSize">The most rows per row group.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <exception cref="NotSupportedException">The schema has a nested column (a struct or a map).</exception>
    public static async Task WriteAsync(
        Stream stream,
        ParquetSchema schema,
        IEnumerable<ParquetRow> rows,
        CompressionMethod codec = CompressionMethod.Snappy,
        int rowGroupSize = ParquetWriteOptions.DefaultRowGroupSize,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(rows);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(rowGroupSize);

        var fields = schema.Fields;

        var builders = fields.Select(field => field is DataField or ListField { Item: DataField }
            ? ParquetColumnBuilders.ForStorage(field)
            : throw new NotSupportedException($"Column '{field.Name}' is a nested type, which cannot be written from rows.")).ToArray();

        try
        {
            var writer = await ParquetWriter.CreateAsync(schema, stream, new ParquetOptions { CompressionMethod = codec }, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            await using (writer.ConfigureAwait(false))
            {
                var count = 0;

                foreach (var row in rows)
                {
                    for (var i = 0; i < fields.Count; i++)
                    {
                        builders[i].Add(row[fields[i].Name]);
                    }

                    if (++count == rowGroupSize)
                    {
                        await FlushAsync(writer, builders, cancellationToken).ConfigureAwait(false);
                        count = 0;
                    }
                }

                if (count > 0)
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
