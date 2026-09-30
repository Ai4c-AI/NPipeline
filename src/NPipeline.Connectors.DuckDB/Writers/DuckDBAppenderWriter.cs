using System.Data.Common;
using DuckDB.NET.Data;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.DuckDB.Writers;

/// <summary>Writes each batch with DuckDB's appender, appending each value with its own type.</summary>
internal sealed class DuckDBAppenderWriter<T>(SqlWriteTarget<T> target, string? schema, string table) : SqlWriter<T>(target)
{
    public override Task WriteAsync(DbConnection connection, DbTransaction? transaction, IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        var values = new object?[Target.QuotedColumns.Count];

        // The appender flushes when it is disposed; it joins the connection's transaction if one is open.
        using (var appender = string.IsNullOrEmpty(schema)
                   ? ((DuckDBConnection)connection).CreateAppender(table)
                   : ((DuckDBConnection)connection).CreateAppender(schema, table))
        {
            foreach (var item in batch)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Target.Plan.Extract(item, values);
                var row = appender.CreateRow();

                foreach (var value in values)
                {
                    Append(row, value);
                }

                row.EndRow();
            }
        }

        return Task.CompletedTask;
    }

    private static void Append(IDuckDBAppenderRow row, object? value)
    {
        _ = value switch
        {
            null => row.AppendNullValue(),
            bool v => row.AppendValue(v),
            sbyte v => row.AppendValue(v),
            byte v => row.AppendValue(v),
            short v => row.AppendValue(v),
            ushort v => row.AppendValue(v),
            int v => row.AppendValue(v),
            uint v => row.AppendValue(v),
            long v => row.AppendValue(v),
            ulong v => row.AppendValue(v),
            float v => row.AppendValue(v),
            double v => row.AppendValue(v),
            decimal v => row.AppendValue(v),
            string v => row.AppendValue(v),
            DateTime v => row.AppendValue(v),
            DateTimeOffset v => row.AppendValue(v),
            DateOnly v => row.AppendValue(v),
            TimeOnly v => row.AppendValue(v),
            TimeSpan v => row.AppendValue(v),
            Guid v => row.AppendValue(v),
            byte[] v => row.AppendValue(v),
            _ => throw new NotSupportedException($"The DuckDB appender cannot write a {value.GetType().Name}."),
        };
    }
}
