using Testcontainers.MySql;

namespace NPipeline.Connectors.MySql.Tests.Fixtures;

/// <summary>
///     Test container fixture for MySQL integration tests.
///     Uses Testcontainers to create a reusable MySQL instance.
///     Requires TESTCONTAINERS_REUSE_ENABLED=true environment variable for reuse.
/// </summary>
public class MySqlTestContainerFixture : IAsyncLifetime
{
    private readonly MySqlContainer _container;

    public MySqlTestContainerFixture()
    {
        _container = new MySqlBuilder("mysql:8.4")
            .WithDatabase("npipeline_test")
            .WithUsername("root")
            .WithPassword("root")
            .WithCommand("--local-infile=1")
            .WithReuse(true)
            .WithLabel("npipeline-test", "mysql-integration")
            .Build();
    }

    // Bulk loads send the file from the client, which the client must allow too.
    public string ConnectionString => _container.GetConnectionString() + ";AllowLoadLocalInfile=true";

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}
