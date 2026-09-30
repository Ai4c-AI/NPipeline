using System.Data.Common;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using NPipeline.Connectors.DuckDB;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.MySql;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.Postgres;
using NPipeline.Connectors.Postgres.Configuration;
using NPipeline.Connectors.SqlServer;
using NPipeline.Connectors.SqlServer.Configuration;
using Npgsql;
using Testcontainers.MsSql;
using Testcontainers.MySql;
using Testcontainers.PostgreSql;

namespace NPipeline.Connectors.Benchmarks.Benchmarks;

/// <summary>
///     Shared setup for database connectors: one table read by <c>Read</c>, and one emptied before each write. The servers
///     run in Docker (reused between runs), so the numbers include the network round trips to a local container; DuckDB
///     runs in process. Each connector uses its default batch size (1,000 rows since phase 5).
/// </summary>
[MemoryDiagnoser]
[Config(typeof(InProcessConfig))]
public abstract class DatabaseConnectorBenchmark
{
    /// <summary>Rows per operation; each record has 20 columns.</summary>
    protected const int Rows = 20_000;

    protected const string InputTable = "bench_input";
    protected const string OutputTable = "bench_output";

    private static readonly (string Member, string Kind)[] Columns =
    [
        ("Id", "int"), ("CustomerId", "int"), ("Quantity", "int"), ("Region", "int"),
        ("OrderNumber", "long"), ("WarehouseId", "long"), ("Sequence", "long"),
        ("Weight", "double"), ("Discount", "double"), ("Score", "double"),
        ("UnitPrice", "decimal"), ("Total", "decimal"),
        ("Name", "text"), ("Email", "text"), ("City", "text"), ("Notes", "text"),
        ("CreatedUtc", "datetime"), ("ShippedUtc", "datetime"), ("Priority", "bool"), ("TrackingId", "guid"),
    ];

    protected string ConnectionString { get; private set; } = string.Empty;

    protected WideRecord[] Records { get; private set; } = [];

    [GlobalSetup]
    public async Task SetupAsync()
    {
        ConnectionString = await StartAsync();
        Records = WideRecord.Generate(Rows);

        foreach (var table in new[] { InputTable, OutputTable })
        {
            await ExecuteAsync($"DROP TABLE IF EXISTS {Quote(table)}");

            var columns = Columns.Select(c => $"{Quote(ColumnName(c.Member))} {TypeName(c.Kind)} NOT NULL");
            await ExecuteAsync($"CREATE TABLE {Quote(table)} ({string.Join(", ", columns)})");
        }

        await WriteBatchAsync(InputTable);

        var read = await ReadAsync();

        if (read != Rows)
            throw new InvalidOperationException($"{GetType().Name} read {read} of {Rows} rows during setup.");
    }

    [IterationSetup(Targets = [nameof(WriteBatch), nameof(WriteBulk)])]
    public void EmptyOutput() => ExecuteAsync($"DELETE FROM {Quote(OutputTable)}").GetAwaiter().GetResult();

    [Benchmark]
    public Task WriteBatch() => WriteBatchAsync(OutputTable);

    /// <summary>The connector's bulk path (<c>SqlBulkCopy</c>, <c>COPY</c>, <c>LOAD DATA</c>, the DuckDB appender).</summary>
    [Benchmark]
    public Task WriteBulk() => WriteBulkAsync(OutputTable);

    [Benchmark]
    public Task<int> Read() => ReadAsync();

    protected abstract Task<string> StartAsync();

    protected abstract Task<DbConnection> OpenAsync();

    protected abstract string Quote(string identifier);

    protected virtual string ColumnName(string member) => member;

    protected abstract string TypeName(string kind);

    protected abstract Task WriteBatchAsync(string table);

    protected abstract Task WriteBulkAsync(string table);

    protected abstract Task<int> ReadAsync();

    protected async Task ExecuteAsync(string sql)
    {
        await using var connection = await OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        _ = await command.ExecuteNonQueryAsync();
    }
}

/// <summary>
///     The file benchmarks' settings, run in process: the benchmark does not rebuild the connectors, so a baseline measures
///     the build it was started from.
/// </summary>
public sealed class InProcessConfig : ManualConfig
{
    public InProcessConfig() =>
        AddJob(Job.Default.WithLaunchCount(1).WithWarmupCount(2).WithIterationCount(8).WithToolchain(InProcessEmitToolchain.Instance));
}

public class SqlServerBenchmarks : DatabaseConnectorBenchmark
{
    protected override async Task<string> StartAsync()
    {
        var container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("Bench@Passw0rd1").WithReuse(true).WithLabel("npipeline-bench", "sqlserver").Build();

        await container.StartAsync();
        return container.GetConnectionString();
    }

    protected override async Task<DbConnection> OpenAsync()
    {
        var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    protected override string Quote(string identifier) => $"[{identifier}]";

    protected override string TypeName(string kind) => kind switch
    {
        "int" => "INT", "long" => "BIGINT", "double" => "FLOAT", "decimal" => "DECIMAL(18, 4)", "text" => "NVARCHAR(200)",
        "datetime" => "DATETIME2(7)", "bool" => "BIT", _ => "UNIQUEIDENTIFIER",
    };

    protected override Task WriteBatchAsync(string table) => NodeRunner.WriteAsync(SqlServerConnector.Sink<WideRecord>(ConnectionString, table), Records);

    protected override Task WriteBulkAsync(string table) =>
        NodeRunner.WriteAsync(SqlServerConnector.Sink<WideRecord>(ConnectionString, table, o => o with { WriteStrategy = SqlServerWriteStrategy.BulkCopy }), Records);

    protected override Task<int> ReadAsync() => NodeRunner.ReadAsync(SqlServerConnector.Source<WideRecord>(ConnectionString, $"SELECT * FROM [{InputTable}]"));
}

public class PostgresBenchmarks : DatabaseConnectorBenchmark
{
    protected override async Task<string> StartAsync()
    {
        var container = new PostgreSqlBuilder("postgres:16-alpine")
            .WithDatabase("bench").WithUsername("bench").WithPassword("bench").WithReuse(true).WithLabel("npipeline-bench", "postgres").Build();

        await container.StartAsync();
        return container.GetConnectionString();
    }

    protected override async Task<DbConnection> OpenAsync()
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    protected override string Quote(string identifier) => $"\"{identifier}\"";

    protected override string ColumnName(string member) => JsonNamingPolicy.SnakeCaseLower.ConvertName(member);

    protected override string TypeName(string kind) => kind switch
    {
        "int" => "INTEGER", "long" => "BIGINT", "double" => "DOUBLE PRECISION", "decimal" => "NUMERIC(18, 4)", "text" => "TEXT",
        "datetime" => "TIMESTAMP", "bool" => "BOOLEAN", _ => "UUID",
    };

    protected override Task WriteBatchAsync(string table) => NodeRunner.WriteAsync(PostgresConnector.Sink<WideRecord>(ConnectionString, table), Records);

    protected override Task WriteBulkAsync(string table) =>
        NodeRunner.WriteAsync(PostgresConnector.Sink<WideRecord>(ConnectionString, table, o => o with { WriteStrategy = PostgresWriteStrategy.Copy }), Records);

    protected override Task<int> ReadAsync() => NodeRunner.ReadAsync(PostgresConnector.Source<WideRecord>(ConnectionString, $"SELECT * FROM \"{InputTable}\""));
}

public class MySqlBenchmarks : DatabaseConnectorBenchmark
{
    protected override async Task<string> StartAsync()
    {
        var container = new MySqlBuilder("mysql:8.4")
            .WithDatabase("bench").WithUsername("root").WithPassword("bench").WithCommand("--local-infile=1")
            .WithReuse(true).WithLabel("npipeline-bench", "mysql").Build();

        await container.StartAsync();
        return container.GetConnectionString() + ";AllowLoadLocalInfile=true";
    }

    protected override async Task<DbConnection> OpenAsync()
    {
        var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    protected override string Quote(string identifier) => $"`{identifier}`";

    protected override string TypeName(string kind) => kind switch
    {
        "int" => "INT", "long" => "BIGINT", "double" => "DOUBLE", "decimal" => "DECIMAL(18, 4)", "text" => "VARCHAR(200)",
        "datetime" => "DATETIME(6)", "bool" => "TINYINT(1)", _ => "CHAR(36)",
    };

    protected override Task WriteBatchAsync(string table) => NodeRunner.WriteAsync(MySqlNodes.Sink<WideRecord>(ConnectionString, table), Records);

    protected override Task WriteBulkAsync(string table) =>
        NodeRunner.WriteAsync(MySqlNodes.Sink<WideRecord>(ConnectionString, table, o => o with { WriteStrategy = MySqlWriteStrategy.BulkLoad }), Records);

    protected override Task<int> ReadAsync() => NodeRunner.ReadAsync(MySqlNodes.Source<WideRecord>(ConnectionString, $"SELECT * FROM `{InputTable}`"));
}

public class DuckDBBenchmarks : DatabaseConnectorBenchmark
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"npipeline-bench-{Environment.ProcessId}.duckdb");

    protected override Task<string> StartAsync() => Task.FromResult(_path);

    [GlobalCleanup]
    public void Cleanup()
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

    protected override string Quote(string identifier) => $"\"{identifier}\"";

    protected override string TypeName(string kind) => kind switch
    {
        "int" => "INTEGER", "long" => "BIGINT", "double" => "DOUBLE", "decimal" => "DECIMAL(18, 4)", "text" => "VARCHAR",
        "datetime" => "TIMESTAMP", "bool" => "BOOLEAN", _ => "UUID",
    };

    protected override Task WriteBatchAsync(string table) =>
        NodeRunner.WriteAsync(DuckDBConnector.Sink<WideRecord>(DuckDBDatabase.File(_path), table, o => o with { WriteStrategy = DuckDBWriteStrategy.Sql }), Records);

    protected override Task WriteBulkAsync(string table) => NodeRunner.WriteAsync(DuckDBConnector.Sink<WideRecord>(DuckDBDatabase.File(_path), table), Records);

    protected override Task<int> ReadAsync() => NodeRunner.ReadAsync(DuckDBConnector.Source<WideRecord>(DuckDBDatabase.File(_path), $"SELECT * FROM \"{InputTable}\""));
}
