using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using NPipeline.Connectors.DuckDB.Attributes;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.DuckDB;

/// <summary>DuckDB's identifiers, parameters, <c>ON CONFLICT</c> and column types.</summary>
internal sealed class DuckDBDialect : SqlDialect
{
    private DuckDBDialect()
    {
    }

    public static DuckDBDialect Instance { get; } = new();

    public override string Name => "duckdb";

    public override int MaxParameters => 10_000;

    // DuckDB binds many parameters slowly; twenty rows a statement wrote 20-column rows fastest (measured at 1, 5, 20 and 100 rows and at the parameter limit).
    // The appender, the default, is faster still.
    public override int MaxRowsPerStatement => 20;

    protected override char OpenQuote => '"';

    protected override char CloseQuote => '"';

    public override string Placeholder(int index) => $"$p{index}";

    public override string ParameterName(int index) => $"p{index}";

    public override string Upsert(string table, IReadOnlyList<string> columns, IReadOnlyList<string> keys, SqlUpsertAction onMatch, int rows)
    {
        var insert = Insert(table, columns, rows);
        var updates = columns.Where(c => !keys.Contains(c, StringComparer.OrdinalIgnoreCase)).Select(c => $"{c} = EXCLUDED.{c}").ToList();

        return onMatch == SqlUpsertAction.Update && updates.Count > 0
            ? $"{insert} ON CONFLICT ({string.Join(", ", keys)}) DO UPDATE SET {string.Join(", ", updates)}"
            : $"{insert} ON CONFLICT ({string.Join(", ", keys)}) DO NOTHING";
    }

    /// <summary>
    ///     <c>CREATE TABLE IF NOT EXISTS</c> for the columns a sink writes. The primary key is the members marked
    ///     <see cref="DuckDBColumnAttribute.PrimaryKey" />, or else the upsert keys; upsert keys that differ from the primary key
    ///     get a <c>UNIQUE</c> constraint, since <c>ON CONFLICT</c> needs one on exactly its keys.
    /// </summary>
    public static string CreateTable<T>(SqlWriteTarget<T> target)
    {
        var text = new StringBuilder($"CREATE TABLE IF NOT EXISTS {target.QualifiedTable} (");
        var primaryKey = new List<string>();

        for (var i = 0; i < target.Plan.Columns.Count; i++)
        {
            var column = target.Plan.Columns[i];

            if (i > 0)
                text.Append(", ");

            text.Append(target.QuotedColumns[i]).Append(' ').Append(TypeName(column.StorageType));

            if (!column.IsNullable)
                text.Append(" NOT NULL");

            if (column.Member?.GetCustomAttribute<DuckDBColumnAttribute>(true)?.PrimaryKey == true)
                primaryKey.Add(target.QuotedColumns[i]);
        }

        var upsertKeys = target.QuotedKeys;

        if (primaryKey.Count == 0)
            primaryKey.AddRange(upsertKeys);
        else if (upsertKeys.Count > 0 && !upsertKeys.ToHashSet(StringComparer.OrdinalIgnoreCase).SetEquals(primaryKey))
            text.Append($", UNIQUE ({string.Join(", ", upsertKeys)})");

        if (primaryKey.Count > 0)
            text.Append($", PRIMARY KEY ({string.Join(", ", primaryKey)})");

        return text.Append(')').ToString();
    }

    private static string TypeName(Type storage) => storage switch
    {
        var t when t == typeof(bool) => "BOOLEAN",
        var t when t == typeof(sbyte) => "TINYINT",
        var t when t == typeof(byte) => "UTINYINT",
        var t when t == typeof(short) => "SMALLINT",
        var t when t == typeof(ushort) => "USMALLINT",
        var t when t == typeof(int) => "INTEGER",
        var t when t == typeof(uint) => "UINTEGER",
        var t when t == typeof(long) => "BIGINT",
        var t when t == typeof(ulong) => "UBIGINT",
        var t when t == typeof(float) => "FLOAT",
        var t when t == typeof(double) => "DOUBLE",
        var t when t == typeof(decimal) => "DECIMAL(38, 18)",
        var t when t == typeof(DateTime) => "TIMESTAMP",
        var t when t == typeof(DateTimeOffset) => "TIMESTAMPTZ",
        var t when t == typeof(DateOnly) => "DATE",
        var t when t == typeof(TimeOnly) => "TIME",
        var t when t == typeof(TimeSpan) => "INTERVAL",
        var t when t == typeof(Guid) => "UUID",
        var t when t == typeof(byte[]) => "BLOB",
        _ => "VARCHAR",
    };
}

/// <summary>The record shapes DuckDB sources and sinks use, cached per naming policy; <c>[DuckDBColumn]</c> names and ignores members.</summary>
internal static class DuckDBShape
{
    private static readonly ConcurrentDictionary<ColumnNamingPolicy, RecordShapeOptions> Cache = new();

    // Static, so options built for the same policy are equal and share the binder's and planner's caches.
    private static readonly Func<MemberInfo, string?> ColumnName = static m => m.GetCustomAttribute<DuckDBColumnAttribute>(true)?.Name;
    private static readonly Func<MemberInfo, bool> IsIgnored = static m => m.GetCustomAttribute<DuckDBColumnAttribute>(true)?.Ignore == true;

    public static RecordShapeOptions For(ColumnNamingPolicy naming) =>
        Cache.GetOrAdd(naming, static policy => new RecordShapeOptions { Naming = policy, ColumnName = ColumnName, IsIgnored = IsIgnored });
}
