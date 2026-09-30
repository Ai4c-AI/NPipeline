using System.Collections.Concurrent;
using System.Data.Common;
using System.Reflection;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Postgres.Mapping;
using NPipeline.Connectors.Sql;
using Npgsql;
using NpgsqlTypes;

namespace NPipeline.Connectors.Postgres;

/// <summary>PostgreSQL's identifiers, positional parameters and <c>ON CONFLICT</c>.</summary>
internal sealed class PostgresDialect : SqlDialect
{
    private static readonly ConcurrentDictionary<MemberInfo, PostgresColumnAttribute?> Attributes = new();

    private PostgresDialect()
    {
    }

    public static PostgresDialect Instance { get; } = new();

    public override string Name => "postgres";

    // The wire protocol numbers parameters with 16 bits.
    public override int MaxParameters => 65_535;

    // Date and time values must match the column's type: a UTC DateTime sent as timestamptz into a timestamp column is
    // shifted by the server's time zone.
    public override bool NeedsColumnTypes => true;

    protected override char OpenQuote => '"';

    protected override char CloseQuote => '"';

    // Positional parameters are sent as they are; named ones make Npgsql rewrite the SQL for every command.
    public override string Placeholder(int index) => $"${index + 1}";

    public override string ParameterName(int index) => string.Empty;

    public override void Bind(DbParameter parameter, SqlColumn column, string? databaseType, object? value)
    {
        var npgsql = (NpgsqlParameter)parameter;

        if (column.Member is { } member && Attributes.GetOrAdd(member, static m => m.GetCustomAttribute<PostgresColumnAttribute>(true)) is { DbType: { } dbType })
        {
            npgsql.NpgsqlDbType = dbType;
            npgsql.Value = value ?? DBNull.Value;
            return;
        }

        var (type, converted) = Normalize(value, databaseType);

        if (type is { } npgsqlType)
            npgsql.NpgsqlDbType = npgsqlType;
        else
            npgsql.ResetDbType();

        npgsql.Value = converted ?? DBNull.Value;
    }

    public override string Upsert(string table, IReadOnlyList<string> columns, IReadOnlyList<string> keys, SqlUpsertAction onMatch, int rows)
    {
        var insert = Insert(table, columns, rows);
        var updates = columns.Where(c => !keys.Contains(c, StringComparer.OrdinalIgnoreCase)).Select(c => $"{c} = EXCLUDED.{c}").ToList();

        return onMatch == SqlUpsertAction.Update && updates.Count > 0
            ? $"{insert} ON CONFLICT ({string.Join(", ", keys)}) DO UPDATE SET {string.Join(", ", updates)}"
            : $"{insert} ON CONFLICT ({string.Join(", ", keys)}) DO NOTHING";
    }

    /// <summary>
    ///     The type to send a value as, and the value adjusted for it. DateTimes follow the column: a <c>timestamp</c>
    ///     stores the UTC wall-clock time, a <c>timestamptz</c> the instant (an unspecified kind counts as UTC).
    ///     DateTimeOffsets are sent as their UTC instant, since PostgreSQL stores no offset. Other types are inferred.
    /// </summary>
    internal static (NpgsqlDbType? Type, object? Value) Normalize(object? value, string? databaseType) =>
        value switch
        {
            DateTime dateTime when databaseType is "timestamp without time zone" => (NpgsqlDbType.Timestamp, DateTime.SpecifyKind(Utc(dateTime), DateTimeKind.Unspecified)),
            DateTime dateTime when databaseType is "timestamp with time zone" => (NpgsqlDbType.TimestampTz, Utc(dateTime)),
            DateTime dateTime when databaseType is "date" => (NpgsqlDbType.Date, DateOnly.FromDateTime(dateTime)),
            DateTimeOffset offset when databaseType is "timestamp without time zone" => (NpgsqlDbType.Timestamp, offset.UtcDateTime.ToUnspecified()),
            DateTimeOffset offset => (NpgsqlDbType.TimestampTz, offset.ToUniversalTime()),
            TimeSpan span when databaseType is "time without time zone" => (NpgsqlDbType.Time, span),
            byte small => (null, (short)small),
            sbyte small => (null, (short)small),
            ushort unsigned => (null, (int)unsigned),
            uint unsigned => (null, (long)unsigned),
            _ => (null, value),
        };

    private static DateTime Utc(DateTime value) =>
        value.Kind switch
        {
            DateTimeKind.Local => value.ToUniversalTime(),
            DateTimeKind.Unspecified => DateTime.SpecifyKind(value, DateTimeKind.Utc),
            _ => value,
        };
}

internal static class PostgresDateTimeExtensions
{
    public static DateTime ToUnspecified(this DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
}

/// <summary>The record shapes PostgreSQL sources and sinks use, cached per naming policy.</summary>
internal static class PostgresShape
{
    private static readonly ConcurrentDictionary<ColumnNamingPolicy, RecordShapeOptions> Cache = new();

    public static RecordShapeOptions For(ColumnNamingPolicy naming) => Cache.GetOrAdd(naming, static policy => new RecordShapeOptions { Naming = policy });
}
