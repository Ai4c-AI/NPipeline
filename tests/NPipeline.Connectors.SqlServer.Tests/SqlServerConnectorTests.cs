using System.Data;
using AwesomeAssertions;
using Microsoft.Data.SqlClient;
using NPipeline.Connectors.Sql;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.Connectors.SqlServer.Mapping;
using NPipeline.Connectors.SqlServer.Tests.Fixtures;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.SqlServer.Tests;

/// <summary>What only SQL Server does; the round trips every database shares are in the database round-trip suite.</summary>
[Collection("SqlServer")]
public sealed class SqlServerConnectorTests(SqlServerTestContainerFixture database)
{
    private readonly string _connectionString = database.ConnectionString;

    [Theory]
    [InlineData(SqlUpsertAction.Update, "new")]
    [InlineData(SqlUpsertAction.Ignore, "old")]
    public async Task Upserts_merge_on_the_keys(SqlUpsertAction onMatch, string expected)
    {
        var table = await CreateAsync("Id INT PRIMARY KEY, Name NVARCHAR(50) NOT NULL");
        await ExecuteAsync($"INSERT INTO {table} VALUES (1, 'old')");

        await WriteAsync(SqlServerConnector.Sink<Named>(_connectionString, table, o => o with { Upsert = new SqlUpsert(["Id"], onMatch) }),
            [new Named(1, "new"), new Named(2, "two")]);

        (await ReadAsync(SqlServerConnector.Source<Named>(_connectionString, $"SELECT * FROM {table} ORDER BY Id")))
            .Should().Equal(new Named(1, expected), new Named(2, "two"));
    }

    [Fact]
    public async Task Identity_members_are_read_but_not_written()
    {
        var table = await CreateAsync("Id INT IDENTITY(1, 1) PRIMARY KEY, Name NVARCHAR(50) NOT NULL");

        await WriteAsync(SqlServerConnector.Sink<WithIdentity>(_connectionString, table), [new WithIdentity { Name = "a" }, new WithIdentity { Name = "b" }]);

        (await ReadAsync(SqlServerConnector.Source<WithIdentity>(_connectionString, $"SELECT * FROM {table} ORDER BY Id")))
            .Select(r => (r.Id, r.Name)).Should().Equal((1, "a"), (2, "b"));
    }

    [Fact]
    public async Task Column_attributes_type_the_parameters()
    {
        var table = await CreateAsync("Id INT PRIMARY KEY, Code VARCHAR(10) NOT NULL");

        await WriteAsync(SqlServerConnector.Sink<Coded>(_connectionString, table), [new Coded { Id = 1, Code = "ab" }]);

        (await ReadAsync(SqlServerConnector.Source<Coded>(_connectionString, $"SELECT * FROM {table}"))).Single().Code.Should().Be("ab");
    }

    [Theory]
    [InlineData(SqlTransactionMode.PerBatch)]
    [InlineData(SqlTransactionMode.None)]
    [InlineData(SqlTransactionMode.WholeRun)]
    public async Task Bulk_copy_writes_inside_or_outside_a_transaction(SqlTransactionMode transaction)
    {
        var table = await CreateAsync("Id INT PRIMARY KEY, Name NVARCHAR(50) NULL");
        var rows = Enumerable.Range(1, 250).Select(i => new Maybe(i, i % 3 == 0 ? null : $"n{i}")).ToList();

        await WriteAsync(SqlServerConnector.Sink<Maybe>(_connectionString, table,
            o => o with { WriteStrategy = SqlServerWriteStrategy.BulkCopy, Transaction = transaction, BatchSize = 100 }), rows);

        (await ReadAsync(SqlServerConnector.Source<Maybe>(_connectionString, $"SELECT * FROM {table} ORDER BY Id"))).Should().Equal(rows);
    }

    [Fact]
    public async Task Wide_batches_stay_under_the_parameter_limit()
    {
        // 30 columns of 1,000-row batches would need 30,000 parameters in one statement.
        var columns = string.Join(", ", Enumerable.Range(0, 30).Select(i => $"C{i} INT NOT NULL"));
        var table = await CreateAsync(columns);
        var rows = Enumerable.Range(0, 1_000).Select(Wide.Create).ToList();

        await WriteAsync(SqlServerConnector.Sink<Wide>(_connectionString, table, o => o with { BatchSize = 1_000 }), rows);

        (await CountAsync(table)).Should().Be(1_000);
    }

    [Fact]
    public async Task Writes_to_a_schema_through_a_named_pooled_connection()
    {
        await ExecuteAsync("IF SCHEMA_ID('sales') IS NULL EXEC('CREATE SCHEMA sales')");
        var table = $"t_{Guid.NewGuid():N}"[..20];
        await ExecuteAsync($"CREATE TABLE sales.{table} (Id INT PRIMARY KEY, Name NVARCHAR(50) NOT NULL)");
        var pool = new SqlServerConnectionPool(new Dictionary<string, string> { ["reporting"] = _connectionString });

        await WriteAsync(SqlServerConnector.Sink<Named>(pool, table, o => o with { Schema = "sales", ConnectionName = "reporting" }), [new Named(1, "a")]);

        (await ReadAsync(SqlServerConnector.Source<Named>(pool, $"SELECT * FROM sales.{table}", o => o with { ConnectionName = "reporting" })))
            .Should().Equal(new Named(1, "a"));
    }

    [Fact]
    public void Bulk_copy_cannot_upsert()
    {
        var create = () => SqlServerConnector.Sink<Named>(_connectionString, "t", o => o with { WriteStrategy = SqlServerWriteStrategy.BulkCopy, Upsert = SqlUpsert.On("Id") });

        create.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void The_upsert_is_a_merge_with_holdlock()
    {
        var sql = SqlServerDialect.Instance.Upsert("[dbo].[t]", ["[Id]", "[Name]"], ["[Id]"], SqlUpsertAction.Update, 2);

        sql.Should().Be("MERGE INTO [dbo].[t] WITH (HOLDLOCK) AS target USING (VALUES (@p0, @p1), (@p2, @p3)) AS source ([Id], [Name])"
                        + " ON target.[Id] = source.[Id] WHEN MATCHED THEN UPDATE SET [Name] = source.[Name]"
                        + " WHEN NOT MATCHED THEN INSERT ([Id], [Name]) VALUES (source.[Id], source.[Name]);");
    }

    private async Task<string> CreateAsync(string columns)
    {
        var table = $"t_{Guid.NewGuid():N}"[..20];
        await ExecuteAsync($"CREATE TABLE {table} ({columns})");
        return table;
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        _ = await command.ExecuteNonQueryAsync();
    }

    private async Task<int> CountAsync(string table)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"SELECT COUNT(*) FROM {table}", connection);
        return (int)(await command.ExecuteScalarAsync())!;
    }

    private static async Task WriteAsync<T>(SinkNode<T> sink, IEnumerable<T> items)
    {
        await using var input = new InMemoryDataStream<T>([.. items]);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
    }

    private static async Task<List<T>> ReadAsync<T>(SourceNode<T> source)
    {
        var items = new List<T>();

        await foreach (var item in source.OpenStream(new PipelineContext(), CancellationToken.None))
        {
            items.Add(item);
        }

        return items;
    }

    public sealed record Named(int Id, string Name);

    public sealed record Maybe(int Id, string? Name);

    public sealed class WithIdentity
    {
        [SqlServerColumn("Id", Identity = true)]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    public sealed class Coded
    {
        public int Id { get; set; }

        [SqlServerColumn("Code", DbType = SqlDbType.VarChar, Size = 10)]
        public string Code { get; set; } = string.Empty;
    }

    public sealed class Wide
    {
        public int C0 { get; set; }
        public int C1 { get; set; }
        public int C2 { get; set; }
        public int C3 { get; set; }
        public int C4 { get; set; }
        public int C5 { get; set; }
        public int C6 { get; set; }
        public int C7 { get; set; }
        public int C8 { get; set; }
        public int C9 { get; set; }
        public int C10 { get; set; }
        public int C11 { get; set; }
        public int C12 { get; set; }
        public int C13 { get; set; }
        public int C14 { get; set; }
        public int C15 { get; set; }
        public int C16 { get; set; }
        public int C17 { get; set; }
        public int C18 { get; set; }
        public int C19 { get; set; }
        public int C20 { get; set; }
        public int C21 { get; set; }
        public int C22 { get; set; }
        public int C23 { get; set; }
        public int C24 { get; set; }
        public int C25 { get; set; }
        public int C26 { get; set; }
        public int C27 { get; set; }
        public int C28 { get; set; }
        public int C29 { get; set; }

        public static Wide Create(int i) => new() { C0 = i, C29 = i };
    }
}
