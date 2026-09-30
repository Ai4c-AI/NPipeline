using System.Collections.Concurrent;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using System.Text;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Snowflake.Mapping;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.Snowflake;

/// <summary>Snowflake's identifiers, positional parameters and <c>MERGE</c>.</summary>
internal sealed class SnowflakeDialect : SqlDialect
{
    private static readonly ConcurrentDictionary<MemberInfo, SnowflakeColumnAttribute?> Attributes = new();

    private SnowflakeDialect()
    {
    }

    public static SnowflakeDialect Instance { get; } = new();

    public override string Name => "snowflake";

    // Snowflake has no documented bind limit; this keeps statements well under its 1 MB text limit.
    public override int MaxParameters => 10_000;

    protected override char OpenQuote => '"';

    protected override char CloseQuote => '"';

    // Snowflake.Data binds "?" placeholders to parameters named by their 1-based position.
    public override string Placeholder(int index) => "?";

    public override string ParameterName(int index) => (index + 1).ToString(CultureInfo.InvariantCulture);

    public override void Bind(DbParameter parameter, SqlColumn column, string? databaseType, object? value)
    {
        if (column.Member is { } member && Attributes.GetOrAdd(member, static m => m.GetCustomAttribute<SnowflakeColumnAttribute>(true)) is { DbTypeNullable: { } dbType } attribute)
        {
            parameter.DbType = dbType;

            if (attribute.SizeNullable is { } size)
                parameter.Size = size;
        }
        else if (StandardDbType(column.StorageType) is { } standard)
        {
            parameter.DbType = standard;
        }

        parameter.Value = value switch
        {
            null => DBNull.Value,

            // The driver has no DateOnly or TimeOnly binding; send them as the types it binds to DATE and TIME.
            DateOnly date => date.ToDateTime(TimeOnly.MinValue),
            TimeOnly time => time.ToTimeSpan(),
            _ => value,
        };
    }

    public override string Upsert(string table, IReadOnlyList<string> columns, IReadOnlyList<string> keys, SqlUpsertAction onMatch, int rows)
    {
        // VALUES names its columns COLUMN1…COLUMNn; the select renames them to the table's.
        var projection = string.Join(", ", columns.Select((c, i) => $"COLUMN{i + 1} AS {c}"));
        var text = new StringBuilder();

        text.Append($"MERGE INTO {table} AS target USING (SELECT {projection} FROM VALUES {Values(columns.Count, rows)}) AS source");
        text.Append($" ON {string.Join(" AND ", keys.Select(k => $"target.{k} = source.{k}"))}");

        var updates = columns.Where(c => !keys.Contains(c, StringComparer.OrdinalIgnoreCase)).Select(c => $"target.{c} = source.{c}").ToList();

        if (onMatch == SqlUpsertAction.Update && updates.Count > 0)
            text.Append($" WHEN MATCHED THEN UPDATE SET {string.Join(", ", updates)}");

        text.Append($" WHEN NOT MATCHED THEN INSERT ({string.Join(", ", columns)}) VALUES ({string.Join(", ", columns.Select(c => $"source.{c}"))})");
        return text.ToString();
    }

    /// <summary>Reads <c>[SnowflakeColumn(Identity = true)]</c>, which leaves a member out of writes.</summary>
    internal static bool IsIdentity(MemberInfo member) => member.GetCustomAttribute<SnowflakeColumnAttribute>(true)?.Identity == true;
}

/// <summary>The record shapes Snowflake sources and sinks use, cached per naming policy.</summary>
internal static class SnowflakeShape
{
    private static readonly ConcurrentDictionary<ColumnNamingPolicy, RecordShapeOptions> Reads = new();
    private static readonly ConcurrentDictionary<ColumnNamingPolicy, RecordShapeOptions> Writes = new();

    // Static, so options built for the same policy are equal and share the binder's and planner's caches.
    private static readonly Func<MemberInfo, bool> IsIdentity = SnowflakeDialect.IsIdentity;

    public static RecordShapeOptions Read(ColumnNamingPolicy naming) => Reads.GetOrAdd(naming, static policy => new RecordShapeOptions { Naming = policy });

    /// <summary>Identity columns are read but never written.</summary>
    public static RecordShapeOptions Write(ColumnNamingPolicy naming) =>
        Writes.GetOrAdd(naming, static policy => new RecordShapeOptions { Naming = policy, IsIgnored = IsIdentity });
}
