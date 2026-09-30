using System.Data.Common;
using MySqlConnector;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.MySql.Writers;

/// <summary>
///     Writes each batch with <c>MySqlBulkCopy</c> (<c>LOAD DATA LOCAL INFILE</c>). The driver serialises the batch's typed
///     values itself, so no culture or quoting is involved.
/// </summary>
internal sealed class MySqlBulkLoadWriter<T>(SqlWriteTarget<T> target) : SqlWriter<T>(target)
{
    public override async Task WriteAsync(DbConnection connection, DbTransaction? transaction, IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        var bulkCopy = new MySqlBulkCopy((MySqlConnection)connection, (MySqlTransaction?)transaction)
        {
            DestinationTableName = Target.QualifiedTable,
            BulkCopyTimeout = Target.CommandTimeout,
        };

        // MySqlBulkCopy quotes destination column names itself.
        for (var i = 0; i < Target.QuotedColumns.Count; i++)
        {
            bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(i, Target.Plan.ColumnNames[i]));
        }

        var reader = new SqlBatchDataReader<T>(Target, batch);
        await using var readerScope = reader.ConfigureAwait(false);
        var result = await bulkCopy.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);

        // LOAD DATA turns a value it cannot store into a warning and writes something else (a zero date, a truncated
        // string), so a warning fails the batch rather than letting a changed value through.
        if (result.Warnings.Count > 0)
        {
            throw new InvalidOperationException(
                $"The bulk load into {Target.QualifiedTable} changed or dropped values: {string.Join("; ", result.Warnings.Take(5).Select(w => w.Message))}");
        }
    }
}
