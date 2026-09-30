using System.Data.Common;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using NPipeline.Connectors.Database.RoundTrip.Tests.Models;
using NPipeline.Connectors.DuckDB;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.MySql;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.Postgres;
using NPipeline.Connectors.Postgres.Configuration;
using NPipeline.Connectors.SqlServer;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Nodes;
using Npgsql;

namespace NPipeline.Connectors.Database.RoundTrip.Tests.Harnesses;

public sealed class SqlServerHarness(string connectionString) : DatabaseHarness
{
    public override string Name => "SQL Server";

    public override string Quote(string identifier) => $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";

    public override string TypeName(ColumnType type) => type switch
    {
        ColumnType.Int => "INT",
        ColumnType.BigInt => "BIGINT",
        ColumnType.Decimal => "DECIMAL(18, 4)",
        ColumnType.Double => "FLOAT",
        ColumnType.Bool => "BIT",
        ColumnType.Text => "NVARCHAR(400)",
        ColumnType.DateTime => "DATETIME2(7)",
        ColumnType.DateTimeOffset => "DATETIMEOFFSET(7)",
        ColumnType.Date => "DATE",
        ColumnType.Time => "TIME(7)",
        ColumnType.Guid => "UNIQUEIDENTIFIER",
        ColumnType.Binary => "VARBINARY(400)",
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };

    protected override async Task<DbConnection> OpenAsync()
    {
        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }

    protected override SinkNode<T> CreateSink<T>(string table, WriteSettings settings) =>
        SqlServerConnector.Sink<T>(connectionString, table, o => Apply(o, settings) with
        {
            WriteStrategy = settings.Mode switch
            {
                WriteMode.PerRow => SqlServerWriteStrategy.PerRow,
                WriteMode.Bulk => SqlServerWriteStrategy.BulkCopy,
                _ => SqlServerWriteStrategy.Batch,
            },
        });

    public override SourceNode<T> CreateSource<T>(string sql, ReadSettings settings) =>
        SqlServerConnector.Source<T>(connectionString, sql, o => Apply(o, settings));
}

public sealed class PostgresHarness(string connectionString) : DatabaseHarness
{
    public override string Name => "Postgres";

    // Postgres maps members to snake_case columns by default.
    public override string ColumnName(string member) => System.Text.Json.JsonNamingPolicy.SnakeCaseLower.ConvertName(member);

    public override string Quote(string identifier) => $"\"{identifier.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";

    public override string TypeName(ColumnType type) => type switch
    {
        ColumnType.Int => "INTEGER",
        ColumnType.BigInt => "BIGINT",
        ColumnType.Decimal => "NUMERIC(18, 4)",
        ColumnType.Double => "DOUBLE PRECISION",
        ColumnType.Bool => "BOOLEAN",
        ColumnType.Text => "TEXT",
        ColumnType.DateTime => "TIMESTAMP",
        ColumnType.DateTimeOffset => "TIMESTAMPTZ",
        ColumnType.Date => "DATE",
        ColumnType.Time => "TIME",
        ColumnType.Guid => "UUID",
        ColumnType.Binary => "BYTEA",
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };

    protected override async Task<DbConnection> OpenAsync()
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }

    protected override SinkNode<T> CreateSink<T>(string table, WriteSettings settings) =>
        PostgresConnector.Sink<T>(connectionString, table, o => Apply(o, settings) with
        {
            WriteStrategy = settings.Mode switch
            {
                WriteMode.PerRow => PostgresWriteStrategy.PerRow,
                WriteMode.Bulk => PostgresWriteStrategy.Copy,
                _ => PostgresWriteStrategy.Batch,
            },
        });

    public override SourceNode<T> CreateSource<T>(string sql, ReadSettings settings) =>
        PostgresConnector.Source<T>(connectionString, sql, o => Apply(o, settings));
}

public sealed class MySqlHarness(string connectionString) : DatabaseHarness
{
    public override string Name => "MySQL";

    public override string Quote(string identifier) => $"`{identifier.Replace("`", "``", StringComparison.Ordinal)}`";

    // MySQL has no type that keeps an offset.
    public override bool Supports(ColumnType type) => type != ColumnType.DateTimeOffset;

    public override string TypeName(ColumnType type) => type switch
    {
        ColumnType.Int => "INT",
        ColumnType.BigInt => "BIGINT",
        ColumnType.Decimal => "DECIMAL(18, 4)",
        ColumnType.Double => "DOUBLE",
        ColumnType.Bool => "TINYINT(1)",
        ColumnType.Text => "VARCHAR(400)",
        ColumnType.DateTime => "DATETIME(6)",
        ColumnType.Date => "DATE",
        ColumnType.Time => "TIME(6)",
        ColumnType.Guid => "CHAR(36)",
        ColumnType.Binary => "VARBINARY(400)",
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };

    protected override async Task<DbConnection> OpenAsync()
    {
        var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }

    protected override SinkNode<T> CreateSink<T>(string table, WriteSettings settings) =>
        MySqlNodes.Sink<T>(connectionString, table, o => Apply(o, settings) with
        {
            WriteStrategy = settings.Mode switch
            {
                WriteMode.PerRow => MySqlWriteStrategy.PerRow,
                WriteMode.Bulk => MySqlWriteStrategy.BulkLoad,
                _ => MySqlWriteStrategy.Batch,
            },
        });

    public override SourceNode<T> CreateSource<T>(string sql, ReadSettings settings) =>
        MySqlNodes.Source<T>(connectionString, sql, o => Apply(o, settings));
}

/// <summary>DuckDB runs in process, on a database file of its own.</summary>
public sealed class DuckDBHarness : DatabaseHarness, IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"npipeline-roundtrip-{Guid.NewGuid():N}.duckdb");

    public override string Name => "DuckDB";

    public override string Quote(string identifier) => $"\"{identifier.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";

    // DuckDB writes with its appender or with SQL; there is no separate per-row strategy.
    public override bool Supports(WriteMode mode) => mode != WriteMode.PerRow;

    public override string TypeName(ColumnType type) => type switch
    {
        ColumnType.Int => "INTEGER",
        ColumnType.BigInt => "BIGINT",
        ColumnType.Decimal => "DECIMAL(18, 4)",
        ColumnType.Double => "DOUBLE",
        ColumnType.Bool => "BOOLEAN",
        ColumnType.Text => "VARCHAR",
        ColumnType.DateTime => "TIMESTAMP",
        ColumnType.DateTimeOffset => "TIMESTAMPTZ",
        ColumnType.Date => "DATE",
        ColumnType.Time => "TIME",
        ColumnType.Guid => "UUID",
        ColumnType.Binary => "BLOB",
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };

    public void Dispose()
    {
        foreach (var file in new[] { _path, _path + ".wal" })
        {
            if (File.Exists(file))
                File.Delete(file);
        }
    }

    protected override async Task<DbConnection> OpenAsync()
    {
        var connection = new DuckDBConnection($"Data Source={_path}");
        await connection.OpenAsync();
        return connection;
    }

    protected override SinkNode<T> CreateSink<T>(string table, WriteSettings settings) =>
        DuckDBConnector.Sink<T>(DuckDBDatabase.File(_path), table, o => Apply(o, settings) with
        {
            WriteStrategy = settings.Mode == WriteMode.Bulk ? DuckDBWriteStrategy.Appender : DuckDBWriteStrategy.Sql,
        });

    public override SourceNode<T> CreateSource<T>(string sql, ReadSettings settings) =>
        DuckDBConnector.Source<T>(DuckDBDatabase.File(_path), sql, o => Apply(o, settings));
}
