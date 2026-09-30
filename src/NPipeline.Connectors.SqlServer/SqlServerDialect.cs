using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Text;
using Microsoft.Data.SqlClient;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Sql;
using NPipeline.Connectors.SqlServer.Mapping;

namespace NPipeline.Connectors.SqlServer;

/// <summary>SQL Server's identifiers, parameters and <c>MERGE</c>.</summary>
internal sealed class SqlServerDialect : SqlDialect
{
    /// <summary>Strings and binary up to this length share one parameter size, so their plans are reused.</summary>
    private const int SharedSize = 4000;

    private static readonly ConcurrentDictionary<MemberInfo, SqlServerColumnAttribute?> Attributes = new();

    private SqlServerDialect()
    {
    }

    public static SqlServerDialect Instance { get; } = new();

    public override string Name => "sqlserver";

    // A command carries at most 2,100 parameters, and sp_executesql uses two of them for the statement and its declaration.
    public override int MaxParameters => 2_098;

    // SQL Server compiles a multi-row VALUES slowly as it grows; ten rows a statement wrote 20-column rows fastest
    // (measured at 1, 5, 10, 25, 50, 100 and 200 rows: 3.6 s for 20,000 rows at 10, against 11.7 s at the parameter limit).
    public override int MaxRowsPerStatement => 10;

    protected override char OpenQuote => '[';

    protected override char CloseQuote => ']';

    public override void Bind(DbParameter parameter, SqlColumn column, string? databaseType, object? value)
    {
        var sql = (SqlParameter)parameter;

        if (column.Member is { } member && Attributes.GetOrAdd(member, static m => m.GetCustomAttribute<SqlServerColumnAttribute>(true)) is { DbTypeNullable: { } dbType } attribute)
        {
            sql.SqlDbType = dbType;

            if (attribute.SizeNullable is { } size)
                sql.Size = size;
        }
        else
        {
            Type(sql, column.StorageType, value);
        }

        sql.Value = value ?? DBNull.Value;
    }

    public override string Upsert(string table, IReadOnlyList<string> columns, IReadOnlyList<string> keys, SqlUpsertAction onMatch, int rows)
    {
        var sourceColumns = string.Join(", ", columns);
        var text = new StringBuilder();

        // HOLDLOCK makes the match and the insert one atomic step, so concurrent upserts of a key cannot both insert it.
        text.Append($"MERGE INTO {table} WITH (HOLDLOCK) AS target USING (VALUES {Values(columns.Count, rows)}) AS source ({sourceColumns})");
        text.Append($" ON {string.Join(" AND ", keys.Select(k => $"target.{k} = source.{k}"))}");

        var updates = columns.Where(c => !keys.Contains(c, StringComparer.OrdinalIgnoreCase)).Select(c => $"{c} = source.{c}").ToList();

        if (onMatch == SqlUpsertAction.Update && updates.Count > 0)
            text.Append($" WHEN MATCHED THEN UPDATE SET {string.Join(", ", updates)}");

        text.Append($" WHEN NOT MATCHED THEN INSERT ({sourceColumns}) VALUES ({string.Join(", ", columns.Select(c => $"source.{c}"))});");
        return text.ToString();
    }

    /// <summary>Types for the storage types whose inference is wrong (dates as <c>datetime</c>) or varies with the value (lengths).</summary>
    private static void Type(SqlParameter parameter, Type storage, object? value)
    {
        switch (storage)
        {
            case var t when t == typeof(string):
                parameter.SqlDbType = SqlDbType.NVarChar;
                parameter.Size = value is string { Length: > SharedSize } ? -1 : SharedSize;
                break;
            case var t when t == typeof(byte[]):
                parameter.SqlDbType = SqlDbType.VarBinary;
                parameter.Size = value is byte[] { Length: > SharedSize * 2 } ? -1 : SharedSize * 2;
                break;
            case var t when t == typeof(DateTime):
                parameter.SqlDbType = SqlDbType.DateTime2;
                break;
            // SqlClient types a decimal by its value, so decimal(5,2) and decimal(6,2) make different statements and each
            // compiles a new plan. A fixed decimal(38, 18) keeps one plan; a value it cannot hold is typed by itself.
            case var t when t == typeof(decimal) && value is not decimal { Scale: > 18 } and not (decimal and (>= 1e20m or <= -1e20m)):
                parameter.SqlDbType = SqlDbType.Decimal;
                parameter.Precision = 38;
                parameter.Scale = 18;
                break;
            case var t when t == typeof(DateTimeOffset):
                parameter.SqlDbType = SqlDbType.DateTimeOffset;
                break;
            case var t when t == typeof(DateOnly):
                parameter.SqlDbType = SqlDbType.Date;
                break;
            case var t when t == typeof(TimeOnly) || t == typeof(TimeSpan):
                parameter.SqlDbType = SqlDbType.Time;
                break;
            default:
                if (StandardDbType(storage) is { } dbType)
                    parameter.DbType = dbType;

                break;
        }
    }

    /// <summary>Reads <c>[SqlServerColumn(Identity = true)]</c>, which leaves a member out of writes.</summary>
    internal static bool IsIdentity(MemberInfo member) => member.GetCustomAttribute<SqlServerColumnAttribute>(true)?.Identity == true;
}

/// <summary>The record shapes SQL Server sources and sinks use, cached per naming policy.</summary>
internal static class SqlServerShape
{
    private static readonly ConcurrentDictionary<ColumnNamingPolicy, RecordShapeOptions> Reads = new();
    private static readonly ConcurrentDictionary<ColumnNamingPolicy, RecordShapeOptions> Writes = new();

    // Static, so options built for the same policy are equal and share the binder's and planner's caches.
    private static readonly Func<MemberInfo, bool> IsIdentity = SqlServerDialect.IsIdentity;

    public static RecordShapeOptions Read(ColumnNamingPolicy naming) => Reads.GetOrAdd(naming, static policy => new RecordShapeOptions { Naming = policy });

    /// <summary>Identity columns are read but never written.</summary>
    public static RecordShapeOptions Write(ColumnNamingPolicy naming) =>
        Writes.GetOrAdd(naming, static policy => new RecordShapeOptions { Naming = policy, IsIgnored = IsIdentity });
}
