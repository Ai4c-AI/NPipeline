using System.Collections.Concurrent;
using System.Reflection;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.MySql.Mapping;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.MySql;

/// <summary>MySQL's identifiers, parameters and <c>ON DUPLICATE KEY UPDATE</c>.</summary>
internal sealed class MySqlDialect : SqlDialect
{
    private MySqlDialect()
    {
    }

    public static MySqlDialect Instance { get; } = new();

    public override string Name => "mysql";

    // The binary protocol numbers parameters with 16 bits.
    public override int MaxParameters => 65_535;

    protected override char OpenQuote => '`';

    protected override char CloseQuote => '`';

    /// <summary>
    ///     MySQL matches rows on the table's primary key and unique indexes, whichever the row collides with; the keys only
    ///     decide which columns are left alone on update. <c>VALUES(column)</c> works on MySQL 8 and MariaDB.
    /// </summary>
    public override string Upsert(string table, IReadOnlyList<string> columns, IReadOnlyList<string> keys, SqlUpsertAction onMatch, int rows)
    {
        var updates = columns.Where(c => !keys.Contains(c, StringComparer.OrdinalIgnoreCase)).Select(c => $"{c} = VALUES({c})").ToList();

        // INSERT IGNORE would also hide other errors (bad values, foreign keys); a no-op update ignores only the duplicate.
        var onDuplicate = onMatch == SqlUpsertAction.Update && updates.Count > 0
            ? string.Join(", ", updates)
            : $"{keys[0]} = {keys[0]}";

        return $"{Insert(table, columns, rows)} ON DUPLICATE KEY UPDATE {onDuplicate}";
    }

    /// <summary>Reads <c>[MySqlColumn(AutoIncrement = true)]</c>, which leaves a member out of writes.</summary>
    internal static bool IsAutoIncrement(MemberInfo member) => member.GetCustomAttribute<MySqlColumnAttribute>(true)?.AutoIncrement == true;
}

/// <summary>The record shapes MySQL sources and sinks use, cached per naming policy.</summary>
internal static class MySqlShape
{
    private static readonly ConcurrentDictionary<ColumnNamingPolicy, RecordShapeOptions> Reads = new();
    private static readonly ConcurrentDictionary<ColumnNamingPolicy, RecordShapeOptions> Writes = new();

    // Static, so options built for the same policy are equal and share the binder's and planner's caches.
    private static readonly Func<MemberInfo, bool> IsAutoIncrement = MySqlDialect.IsAutoIncrement;

    public static RecordShapeOptions Read(ColumnNamingPolicy naming) => Reads.GetOrAdd(naming, static policy => new RecordShapeOptions { Naming = policy });

    /// <summary>Auto-increment columns are read but never written.</summary>
    public static RecordShapeOptions Write(ColumnNamingPolicy naming) =>
        Writes.GetOrAdd(naming, static policy => new RecordShapeOptions { Naming = policy, IsIgnored = IsAutoIncrement });
}
