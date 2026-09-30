using AwesomeAssertions;
using DuckDB.NET.Data;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.DuckDB.Attributes;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.DependencyInjection;
using NPipeline.Connectors.Sql;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.DuckDB.Tests;

/// <summary>What only DuckDB does; the round trips every database shares are in the database round-trip suite.</summary>
public sealed class DuckDBConnectorTests : IDisposable
{
    private readonly string _directory = Directory.CreateTempSubdirectory("npipeline-duckdb-").FullName;

    private DuckDBDatabase Database => DuckDBDatabase.File(Path.Combine(_directory, "test.duckdb"));

    public void Dispose() => Directory.Delete(_directory, true);

    [Theory]
    [InlineData("orders.parquet")]
    [InlineData("orders.csv")]
    [InlineData("orders.json")]
    public async Task Exports_to_a_file_and_reads_it_back(string name)
    {
        var path = Path.Combine(_directory, "out", name);
        Order[] orders = [new(1, "Ada", 12.5m), new(2, "Grace", 7m)];

        await WriteAsync(DuckDBConnector.ToFile<Order>(path), orders);
        var read = await ReadAsync(DuckDBConnector.FromFile<Order>(path));

        read.OrderBy(o => o.Id).Should().Equal(orders);
    }

    [Fact]
    public async Task Reads_a_glob_of_files()
    {
        await WriteAsync(DuckDBConnector.ToFile<Order>(Path.Combine(_directory, "a.parquet")), [new Order(1, "a", 1m)]);
        await WriteAsync(DuckDBConnector.ToFile<Order>(Path.Combine(_directory, "b.parquet")), [new Order(2, "b", 2m)]);

        (await ReadAsync(DuckDBConnector.FromFile<Order>(Path.Combine(_directory, "*.parquet")))).Select(o => o.Id).Order().Should().Equal(1, 2);
    }

    [Fact]
    public async Task Creates_the_table_from_the_record_and_can_truncate_it()
    {
        await WriteAsync(DuckDBConnector.Sink<Order>(Database, "orders"), [new Order(1, "a", 1m)]);
        await WriteAsync(DuckDBConnector.Sink<Order>(Database, "orders", o => o with { TruncateBeforeWrite = true }), [new Order(2, "b", 2m)]);

        (await ReadAsync(DuckDBConnector.Source<Order>(Database, "SELECT * FROM orders"))).Should().Equal(new Order(2, "b", 2m));
    }

    [Fact]
    public async Task The_created_table_marks_non_nullable_members_not_null()
    {
        await WriteAsync(DuckDBConnector.Sink<Order>(Database, "orders"), []);

        var nullable = await ReadAsync(DuckDBConnector.Source(Database, "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = 'orders' ORDER BY ordinal_position",
            row => (row.Get<string>("column_name"), row.Get<string>("is_nullable"))));

        nullable.Should().Equal(("Id", "NO"), ("Name", "YES"), ("Amount", "NO"));
    }

    [Fact]
    public async Task Upserts_with_sql_writes()
    {
        await ExecuteAsync("CREATE TABLE orders (Id INTEGER PRIMARY KEY, Name VARCHAR, Amount DECIMAL(18, 4))");
        await ExecuteAsync("INSERT INTO orders VALUES (1, 'old', 1)");

        await WriteAsync(DuckDBConnector.Sink<Order>(Database, "orders", o => o with { WriteStrategy = DuckDBWriteStrategy.Sql, Upsert = SqlUpsert.On("Id") }),
            [new Order(1, "new", 5m), new Order(2, "two", 2m)]);

        (await ReadAsync(DuckDBConnector.Source<Order>(Database, "SELECT * FROM orders ORDER BY Id"))).Should().Equal(new Order(1, "new", 5m), new Order(2, "two", 2m));
    }

    [Fact]
    public async Task Upserts_into_a_table_it_created_on_the_upsert_keys()
    {
        var sink = () => DuckDBConnector.Sink<Order>(Database, "orders", o => o with { WriteStrategy = DuckDBWriteStrategy.Sql, Upsert = SqlUpsert.On("Id") });

        await WriteAsync(sink(), [new Order(1, "old", 1m)]);
        await WriteAsync(sink(), [new Order(1, "new", 5m), new Order(2, "two", 2m)]);

        (await ReadAsync(DuckDBConnector.Source<Order>(Database, "SELECT * FROM orders ORDER BY Id"))).Should().Equal(new Order(1, "new", 5m), new Order(2, "two", 2m));
        (await KeysAsync("orders")).Should().Equal("PRIMARY KEY (Id)");
    }

    [Fact]
    public async Task The_created_table_takes_its_primary_key_from_the_attribute()
    {
        await WriteAsync(DuckDBConnector.Sink<Keyed>(Database, "keyed"), [new Keyed { Id = 1, Code = "a" }]);

        (await KeysAsync("keyed")).Should().Equal("PRIMARY KEY (Id)");
    }

    [Fact]
    public async Task Upsert_keys_other_than_the_primary_key_get_a_unique_constraint()
    {
        var sink = () => DuckDBConnector.Sink<Keyed>(Database, "keyed", o => o with { WriteStrategy = DuckDBWriteStrategy.Sql, Upsert = SqlUpsert.On("Code") });

        await WriteAsync(sink(), [new Keyed { Id = 1, Code = "a", Name = "old" }]);
        await WriteAsync(sink(), [new Keyed { Id = 1, Code = "a", Name = "new" }]);

        (await ReadAsync(DuckDBConnector.Source(Database, "SELECT Name FROM keyed", row => row.Get<string>(0)))).Should().Equal("new");
        (await KeysAsync("keyed")).Should().Equal("PRIMARY KEY (Id)", "UNIQUE (Code)");
    }

    [Fact]
    public async Task Honours_the_column_attribute()
    {
        await WriteAsync(DuckDBConnector.Sink<Attributed>(Database, "people"), [new Attributed { Id = 1, Secret = "x" }]);

        var columns = await ReadAsync(DuckDBConnector.Source(Database, "SELECT column_name FROM information_schema.columns WHERE table_name = 'people'", row => row.Get<string>(0)));

        columns.Should().Equal("person_id");
    }

    [Fact]
    public async Task Applies_settings_to_each_connection()
    {
        var database = Database with { Threads = 2, Settings = new Dictionary<string, string> { ["default_order"] = "DESC" } };

        var threads = await ReadAsync(DuckDBConnector.Source(database, "SELECT current_setting('threads') AS t", row => row.Get<long>(0)));

        threads.Should().Equal(2L);
    }

    [Fact]
    public void Setting_names_must_be_identifiers()
    {
        var create = () => DuckDBConnector.Source<Order>(Database with { Settings = new Dictionary<string, string> { ["x; DROP TABLE y"] = "1" } }, "SELECT 1");

        create.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void The_appender_cannot_upsert()
    {
        var create = () => DuckDBConnector.Sink<Order>(Database, "orders", o => o with { Upsert = SqlUpsert.On("Id") });

        create.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public async Task Dependency_injection_resolves_named_databases()
    {
        var services = new ServiceCollection().AddDuckDBConnector().AddDuckDBDatabase("warehouse", Database).BuildServiceProvider();
        var sinks = services.GetRequiredService<DuckDBSinkNodeFactory>();
        var sources = services.GetRequiredService<DuckDBSourceNodeFactory>();

        await WriteAsync(sinks.CreateSink<Order>("orders", "warehouse"), [new Order(1, "a", 1m)]);

        (await ReadAsync(sources.CreateSource<Order>("SELECT * FROM orders", "warehouse"))).Should().ContainSingle();
    }

    private Task<List<string>> KeysAsync(string table) =>
        ReadAsync(DuckDBConnector.Source(Database,
            $"SELECT constraint_type || ' (' || array_to_string(constraint_column_names, ', ') || ')' FROM duckdb_constraints() " +
            $"WHERE table_name = '{table}' AND constraint_type IN ('PRIMARY KEY', 'UNIQUE') ORDER BY constraint_type",
            row => row.Get<string>(0)));

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new DuckDBConnection(Database.ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
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

    public sealed record Order(int Id, string? Name, decimal Amount);

    public sealed class Keyed
    {
        [DuckDBColumn(PrimaryKey = true)]
        public int Id { get; set; }

        public string Code { get; set; } = string.Empty;

        public string? Name { get; set; }
    }

    public sealed class Attributed
    {
        [DuckDBColumn("person_id")]
        public int Id { get; set; }

        [DuckDBColumn(Ignore = true)]
        public string Secret { get; set; } = string.Empty;
    }
}
