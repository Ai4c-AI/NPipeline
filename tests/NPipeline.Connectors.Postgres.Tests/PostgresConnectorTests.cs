using AwesomeAssertions;
using Npgsql;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Postgres.Configuration;
using NPipeline.Connectors.Postgres.Connection;
using NPipeline.Connectors.Postgres.Tests.Fixtures;
using NPipeline.Connectors.Sql;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Postgres.Tests;

/// <summary>What only PostgreSQL does; the round trips every database shares are in the database round-trip suite.</summary>
[Collection("PostgresTestCollection")]
public sealed class PostgresConnectorTests(PostgresTestContainerFixture database)
{
    private readonly string _connectionString = database.ConnectionString;

    [Theory]
    [InlineData(SqlUpsertAction.Update, "new")]
    [InlineData(SqlUpsertAction.Ignore, "old")]
    public async Task Upserts_on_conflict_with_the_keys(SqlUpsertAction onMatch, string expected)
    {
        var table = await CreateAsync("id INTEGER PRIMARY KEY, name TEXT NOT NULL");
        await ExecuteAsync($"INSERT INTO {table} VALUES (1, 'old')");

        await WriteAsync(PostgresConnector.Sink<Named>(_connectionString, table, o => o with { Upsert = new SqlUpsert(["id"], onMatch) }),
            [new Named(1, "new"), new Named(2, "two")]);

        (await ReadAsync(PostgresConnector.Source<Named>(_connectionString, $"SELECT * FROM {table} ORDER BY id")))
            .Should().Equal(new Named(1, expected), new Named(2, "two"));
    }

    [Theory]
    [InlineData(PostgresWriteStrategy.Batch)]
    [InlineData(PostgresWriteStrategy.Copy)]
    public async Task Timestamps_follow_the_column_type_whatever_the_session_time_zone(PostgresWriteStrategy strategy)
    {
        // A session in Brisbane: a UTC value sent as timestamptz into a timestamp column would be shifted by ten hours.
        var brisbane = new NpgsqlConnectionStringBuilder(_connectionString) { Timezone = "Australia/Brisbane" }.ConnectionString;
        var table = await CreateAsync("id INTEGER PRIMARY KEY, local_at TIMESTAMP NOT NULL, instant TIMESTAMPTZ NOT NULL");
        var at = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);

        await WriteAsync(PostgresConnector.Sink<Stamped>(brisbane, table, o => o with { WriteStrategy = strategy }), [new Stamped(1, at, at)]);

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT local_at::text, instant AT TIME ZONE 'UTC' FROM {table}", connection);
        await using var reader = await command.ExecuteReaderAsync();
        _ = await reader.ReadAsync();

        reader.GetString(0).Should().Be("2026-01-02 03:04:05");
        reader.GetDateTime(1).Should().Be(new DateTime(2026, 1, 2, 3, 4, 5));
    }

    [Fact]
    public async Task Many_sources_do_not_exhaust_the_servers_connections()
    {
        // The server allows 100 connections; before phase 5 every source opened a data source it never disposed.
        var table = await CreateAsync("id INTEGER PRIMARY KEY, name TEXT NOT NULL");
        await ExecuteAsync($"INSERT INTO {table} VALUES (1, 'a')");

        for (var i = 0; i < 150; i++)
        {
            (await ReadAsync(PostgresConnector.Source<Named>(_connectionString, $"SELECT * FROM {table}"))).Should().ContainSingle();
        }
    }

    [Fact]
    public async Task Members_map_to_snake_case_unless_told_otherwise()
    {
        var table = await CreateAsync("order_id INTEGER PRIMARY KEY, \"CustomerName\" TEXT NULL");
        await WriteAsync(PostgresConnector.Sink<SnakeOrder>(_connectionString, table), [new SnakeOrder { OrderId = 1 }]);
        await ExecuteAsync($"UPDATE {table} SET \"CustomerName\" = 'Ada'");

        var read = await ReadAsync(PostgresConnector.Source<PascalOrder>(_connectionString, $"SELECT \"CustomerName\" FROM {table}",
            o => o with { Naming = ColumnNamingPolicy.AsIs }));

        read.Single().CustomerName.Should().Be("Ada");
    }

    [Fact]
    public async Task Reads_and_writes_through_a_named_pooled_data_source()
    {
        var table = await CreateAsync("id INTEGER PRIMARY KEY, name TEXT NOT NULL");
        await using var pool = new PostgresConnectionPool(new PostgresOptions { NamedConnections = { ["reporting"] = _connectionString } });

        await WriteAsync(PostgresConnector.Sink<Named>(pool, table, o => o with { ConnectionName = "reporting" }), [new Named(1, "a")]);

        (await ReadAsync(PostgresConnector.Source<Named>(pool, $"SELECT * FROM {table}", o => o with { ConnectionName = "reporting" })))
            .Should().Equal(new Named(1, "a"));
    }

    [Fact]
    public void Copy_cannot_upsert()
    {
        var create = () => PostgresConnector.Sink<Named>(_connectionString, "t", o => o with { WriteStrategy = PostgresWriteStrategy.Copy, Upsert = SqlUpsert.On("id") });

        create.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void The_upsert_is_insert_on_conflict()
    {
        var sql = PostgresDialect.Instance.Upsert("\"t\"", ["\"id\"", "\"name\""], ["\"id\""], SqlUpsertAction.Update, 2);

        sql.Should().Be("INSERT INTO \"t\" (\"id\", \"name\") VALUES ($1, $2), ($3, $4) ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\"");
    }

    private async Task<string> CreateAsync(string columns)
    {
        var table = $"t_{Guid.NewGuid():N}"[..20];
        await ExecuteAsync($"CREATE TABLE {table} ({columns})");
        return table;
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        _ = await command.ExecuteNonQueryAsync();
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

    public sealed record Stamped(int Id, DateTime LocalAt, DateTime Instant);

    public sealed class SnakeOrder
    {
        public int OrderId { get; set; }
    }

    public sealed class PascalOrder
    {
        public string CustomerName { get; set; } = string.Empty;
    }
}
