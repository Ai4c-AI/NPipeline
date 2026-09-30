using System.Data.Common;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.Snowflake.Writers;

/// <summary>
///     Writes each batch as a CSV file uploaded to an internal stage with <c>PUT</c> and loaded with <c>COPY INTO</c>.
///     A retried batch uploads and loads the same file name, so Snowflake's load metadata skips a file it already loaded.
/// </summary>
internal sealed class SnowflakeStagedCopyWriter<T> : SqlWriter<T>
{
    private readonly ConditionalWeakTable<IReadOnlyList<T>, string> _fileNames = new();
    private readonly string _prefix;
    private readonly bool _purge;
    private readonly string _stage;
    private readonly string _writerId = Guid.NewGuid().ToString("N")[..12];
    private int _fileCounter;

    public SnowflakeStagedCopyWriter(SqlWriteTarget<T> target, string stage, string prefix, bool purge)
        : base(target)
    {
        _stage = stage == "~" ? "@~" : $"@{stage}";
        _prefix = prefix;
        _purge = purge;
    }

    public override async Task WriteAsync(DbConnection connection, DbTransaction? transaction, IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        // The same batch keeps its file name across retries, so a retry after a lost reply is skipped by the load metadata.
        var firstAttempt = !_fileNames.TryGetValue(batch, out var fileName);

        if (firstAttempt)
        {
            fileName = $"{_prefix}{DateTime.UtcNow:yyyyMMddHHmmss}_{_writerId}_{Interlocked.Increment(ref _fileCounter)}.csv";
            _fileNames.AddOrUpdate(batch, fileName);
        }

        var localPath = Path.Combine(Path.GetTempPath(), fileName!);
        var stagePath = $"{_stage}/{fileName}";

        try
        {
            await WriteFileAsync(localPath, batch, cancellationToken).ConfigureAwait(false);

            // OVERWRITE lets a retry replace a partial upload of the same file.
            var put = $"PUT 'file://{localPath.Replace('\\', '/')}' '{stagePath}' AUTO_COMPRESS=TRUE OVERWRITE=TRUE";

            foreach (var row in await QueryAsync(connection, transaction, put, cancellationToken).ConfigureAwait(false))
            {
                var status = row.GetValueOrDefault("status");

                if (!string.Equals(status, "UPLOADED", StringComparison.OrdinalIgnoreCase) && !string.Equals(status, "SKIPPED", StringComparison.OrdinalIgnoreCase))
                    throw new InvalidOperationException($"PUT of '{fileName}' to {_stage} reported status '{status}': {row.GetValueOrDefault("message")}");
            }

            var copy = new StringBuilder()
                .Append($"COPY INTO {Target.QualifiedTable} ({string.Join(", ", Target.QuotedColumns)}) FROM '{stagePath}'")

                // An unquoted empty field is NULL and a quoted one an empty string, which is how the file is written.
                .Append(" FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' EMPTY_FIELD_AS_NULL = TRUE ESCAPE_UNENCLOSED_FIELD = NONE")
                .Append(" BINARY_FORMAT = 'HEX' COMPRESSION = 'GZIP')")

                // A row Snowflake cannot load fails the batch; skipping it would drop data without a trace.
                .Append(" ON_ERROR = 'ABORT_STATEMENT'")
                .Append(_purge ? " PURGE = TRUE" : " PURGE = FALSE")
                .ToString();

            var loaded = await QueryAsync(connection, transaction, copy, cancellationToken).ConfigureAwait(false);

            // On a first attempt the file must have been loaded; nothing loaded means the staged file was not found. A retry
            // may load nothing because the lost attempt already loaded it.
            if (firstAttempt && !loaded.Any(row => !string.IsNullOrEmpty(row.GetValueOrDefault("file"))))
                throw new InvalidOperationException($"COPY INTO {Target.QualifiedTable} from '{stagePath}' loaded no files.");
        }
        finally
        {
            try
            {
                File.Delete(localPath);
            }
            catch (IOException)
            {
                // Best effort: the file is in the temp directory.
            }
        }
    }

    /// <summary>
    ///     One CSV field: <c>NULL</c> as an unquoted empty field, text quoted with doubled quotes, numbers and dates
    ///     culture-invariant, dates to the tick, binary as hex.
    /// </summary>
    internal static string Field(object? value)
    {
        switch (value)
        {
            case null:
                return string.Empty;
            case string text:
                return $"\"{text.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
            case bool flag:
                return flag ? "TRUE" : "FALSE";
            case byte[] bytes:
                return Convert.ToHexString(bytes);
            case DateTime dateTime:
                return dateTime.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
            case DateTimeOffset offset:
                return offset.ToString("yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture);
            case DateOnly date:
                return date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            case TimeOnly time:
                return time.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
            case TimeSpan span:
                return span.ToString("c", CultureInfo.InvariantCulture);
            case IFormattable formattable:
                return formattable.ToString(null, CultureInfo.InvariantCulture);
            default:
                return $"\"{value.ToString()?.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
        }
    }

    private async Task WriteFileAsync(string path, IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        var values = new object?[Target.QuotedColumns.Count];
        var writer = new StreamWriter(path, false, new UTF8Encoding(false), 64 * 1024);

        await using (writer.ConfigureAwait(false))
        {
            var line = new StringBuilder();

            foreach (var item in batch)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Target.Plan.Extract(item, values);
                line.Clear();

                for (var i = 0; i < values.Length; i++)
                {
                    if (i > 0)
                        line.Append(',');

                    line.Append(Field(values[i]));
                }

                line.Append('\n');
                await writer.WriteAsync(line, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task<List<Dictionary<string, string?>>> QueryAsync(DbConnection connection, DbTransaction? transaction, string sql, CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();

        await using (command.ConfigureAwait(false))
        {
            command.Transaction = transaction;
            command.CommandText = sql;
            command.CommandTimeout = Target.CommandTimeout;

            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            await using (reader.ConfigureAwait(false))
            {
                var rows = new List<Dictionary<string, string?>>();

                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        row[reader.GetName(i)] = reader.IsDBNull(i) ? null : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture);
                    }

                    rows.Add(row);
                }

                return rows;
            }
        }
    }
}
