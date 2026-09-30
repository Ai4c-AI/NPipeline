using Testcontainers.MsSql;
using Testcontainers.MySql;
using Testcontainers.PostgreSql;

namespace NPipeline.Connectors.Database.RoundTrip.Tests.Infrastructure;

/// <summary>One container per database for the whole run, reused between runs when Testcontainers reuse is enabled.</summary>
public sealed class SqlServerContainerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
        .WithPassword("RoundTrip@Passw0rd")
        .WithReuse(true)
        .WithLabel("npipeline-test", "database-roundtrip-sqlserver")
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => Task.CompletedTask;
}

public sealed class PostgresContainerFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:16-alpine")
        .WithDatabase("roundtrip")
        .WithUsername("roundtrip")
        .WithPassword("roundtrip")
        .WithReuse(true)
        .WithLabel("npipeline-test", "database-roundtrip-postgres")
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => Task.CompletedTask;
}

public sealed class MySqlContainerFixture : IAsyncLifetime
{
    private readonly MySqlContainer _container = new MySqlBuilder("mysql:8.4")
        .WithDatabase("roundtrip")
        .WithUsername("root")
        .WithPassword("roundtrip")
        .WithCommand("--local-infile=1")
        .WithReuse(true)
        .WithLabel("npipeline-test", "database-roundtrip-mysql")
        .Build();

    // Bulk loads send the file from the client, which the client must allow too.
    public string ConnectionString => _container.GetConnectionString() + ";AllowLoadLocalInfile=true";

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => Task.CompletedTask;
}

[CollectionDefinition(Name)]
public sealed class SqlServerDatabases : ICollectionFixture<SqlServerContainerFixture>
{
    public const string Name = "SQL Server round trips";
}

[CollectionDefinition(Name)]
public sealed class PostgresDatabases : ICollectionFixture<PostgresContainerFixture>
{
    public const string Name = "Postgres round trips";
}

[CollectionDefinition(Name)]
public sealed class MySqlDatabases : ICollectionFixture<MySqlContainerFixture>
{
    public const string Name = "MySQL round trips";
}
