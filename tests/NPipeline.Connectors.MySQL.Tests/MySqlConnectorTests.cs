using AwesomeAssertions;
using MySqlConnector;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Mapping;
using NPipeline.Connectors.MySql.Tests.Fixtures;
using NPipeline.Connectors.Sql;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.MySql.Tests;

/// <summary>What only MySQL does; the round trips every database shares are in the database round-trip suite.</summary>
[Collection("MySql")]
public sealed class MySqlConnectorTests(MySqlTestContainerFixture database)
{
    private readonly string _connectionString = database.ConnectionString;

    [Theory]
    [InlineData(SqlUpsertAction.Update, "new")]
    [InlineData(SqlUpsertAction.Ignore, "old")]
    public async Task Upserts_on_duplicate_keys(SqlUpsertAction onMatch, string expected)
    {
        var table = await CreateAsync("Id INT PRIMARY KEY, Name VARCHAR(50) NOT NULL");
        await ExecuteAsync($"INSERT INTO {table} VALUES (1, 'old')");

        await WriteAsync(MySqlNodes.Sink<Named>(_connectionString, table, o => o with { Upsert = new SqlUpsert(["Id"], onMatch) }),
            [new Named(1, "new"), new Named(2, "two")]);

        (await ReadAsync(MySqlNodes.Source<Named>(_connectionString, $"SELECT * FROM {table} ORDER BY Id")))
            .Should().Equal(new Named(1, expected), new Named(2, "two"));
    }

    [Fact]
    public async Task Auto_increment_members_are_read_but_not_written()
    {
        var table = await CreateAsync("Id INT AUTO_INCREMENT PRIMARY KEY, Name VARCHAR(50) NOT NULL");

        await WriteAsync(MySqlNodes.Sink<WithAutoIncrement>(_connectionString, table), [new WithAutoIncrement { Name = "a" }, new WithAutoIncrement { Name = "b" }]);

        (await ReadAsync(MySqlNodes.Source<WithAutoIncrement>(_connectionString, $"SELECT * FROM {table} ORDER BY Id")))
            .Select(r => (r.Id, r.Name)).Should().Equal((1, "a"), (2, "b"));
    }

    [Fact]
    public async Task A_bulk_load_that_would_change_a_value_fails()
    {
        // LOAD DATA truncates a too-long string with a warning; the sink turns that into a failure.
        var table = await CreateAsync("Id INT PRIMARY KEY, Name VARCHAR(3) NOT NULL");

        var write = () => WriteAsync(MySqlNodes.Sink<Named>(_connectionString, table, o => o with { WriteStrategy = MySqlWriteStrategy.BulkLoad }),
            [new Named(1, "too long")]);

        await write.Should().ThrowAsync<InvalidOperationException>().WithMessage("*changed or dropped values*");
        (await CountAsync(table)).Should().Be(0, "the batch's transaction was rolled back");
    }

    [Fact]
    public void A_bulk_load_cannot_upsert()
    {
        var create = () => MySqlNodes.Sink<Named>(_connectionString, "t", o => o with { WriteStrategy = MySqlWriteStrategy.BulkLoad, Upsert = SqlUpsert.On("Id") });

        create.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void The_upsert_is_on_duplicate_key_update()
    {
        var sql = MySqlDialect.Instance.Upsert("`t`", ["`Id`", "`Name`"], ["`Id`"], SqlUpsertAction.Update, 2);

        sql.Should().Be("INSERT INTO `t` (`Id`, `Name`) VALUES (@p0, @p1), (@p2, @p3) ON DUPLICATE KEY UPDATE `Name` = VALUES(`Name`)");
    }

    private async Task<string> CreateAsync(string columns)
    {
        var table = $"t_{Guid.NewGuid():N}"[..20];
        await ExecuteAsync($"CREATE TABLE {table} ({columns})");
        return table;
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new MySqlCommand(sql, connection);
        _ = await command.ExecuteNonQueryAsync();
    }

    private async Task<long> CountAsync(string table)
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new MySqlCommand($"SELECT COUNT(*) FROM {table}", connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
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

    public sealed class WithAutoIncrement
    {
        [MySqlColumn("Id", AutoIncrement = true)]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
