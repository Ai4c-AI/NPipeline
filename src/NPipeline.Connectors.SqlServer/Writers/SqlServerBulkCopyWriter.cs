using System.Data.Common;
using Microsoft.Data.SqlClient;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.SqlServer.Writers;

/// <summary>Writes each batch with <c>SqlBulkCopy</c>, streaming the batch's values through a data reader instead of a table.</summary>
internal sealed class SqlServerBulkCopyWriter<T>(SqlWriteTarget<T> target, int timeout) : SqlWriter<T>(target)
{
    public override async Task WriteAsync(DbConnection connection, DbTransaction? transaction, IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        // Without the sink's transaction, bulk copy's own keeps a batch all or nothing.
        var options = transaction is null ? SqlBulkCopyOptions.UseInternalTransaction : SqlBulkCopyOptions.Default;
        using var bulkCopy = new SqlBulkCopy((SqlConnection)connection, options, (SqlTransaction?)transaction);

        bulkCopy.DestinationTableName = Target.QualifiedTable;
        bulkCopy.BulkCopyTimeout = timeout;
        bulkCopy.BatchSize = batch.Count;
        bulkCopy.EnableStreaming = true;

        foreach (var column in Target.Plan.ColumnNames)
        {
            _ = bulkCopy.ColumnMappings.Add(column, column);
        }

        var reader = new SqlBatchDataReader<T>(Target, batch);
        await using var readerScope = reader.ConfigureAwait(false);
        await bulkCopy.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);
    }
}
