using System.Data.Common;
using NPipeline.Connectors.Sql;
using Npgsql;

namespace NPipeline.Connectors.Postgres.Writers;

/// <summary>
///     Writes each batch with binary <c>COPY … FROM STDIN</c>. Values go over the wire in PostgreSQL's binary format, typed
///     by each target column, so nothing is formatted as text: no culture, quoting or escaping is involved.
/// </summary>
internal sealed class PostgresCopyWriter<T>(SqlWriteTarget<T> target) : SqlWriter<T>(target)
{
    public override async Task WriteAsync(DbConnection connection, DbTransaction? transaction, IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        var columns = Target.QuotedColumns.Count;
        var values = new object?[columns];
        var sql = $"COPY {Target.QualifiedTable} ({string.Join(", ", Target.QuotedColumns)}) FROM STDIN (FORMAT BINARY)";

        var importer = await ((NpgsqlConnection)connection).BeginBinaryImportAsync(sql, cancellationToken).ConfigureAwait(false);

        await using (importer.ConfigureAwait(false))
        {
            foreach (var item in batch)
            {
                Target.Plan.Extract(item, values);
                await importer.StartRowAsync(cancellationToken).ConfigureAwait(false);

                for (var column = 0; column < columns; column++)
                {
                    var databaseType = DatabaseType(column);
                    var (_, value) = PostgresDialect.Normalize(values[column], databaseType);

                    if (value is null)
                        await importer.WriteNullAsync(cancellationToken).ConfigureAwait(false);
                    else if (databaseType is not null)
                        await importer.WriteAsync(value, databaseType, cancellationToken).ConfigureAwait(false);
                    else
                        await importer.WriteAsync(value, cancellationToken).ConfigureAwait(false);
                }
            }

            _ = await importer.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
