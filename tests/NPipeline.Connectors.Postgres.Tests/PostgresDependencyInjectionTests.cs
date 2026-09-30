using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Postgres.Configuration;
using NPipeline.Connectors.Postgres.DependencyInjection;

namespace NPipeline.Connectors.Postgres.Tests;

public sealed class PostgresDependencyInjectionTests
{
    [Fact]
    public void Connections_added_before_the_connector_keep_its_configuration()
    {
        var services = new ServiceCollection()
            .AddPostgresConnection("reporting", "Host=reporting;Database=sales")
            .AddPostgresConnector(o => o.DefaultConnectionString = "Host=main;Database=sales")
            .BuildServiceProvider();

        var options = services.GetRequiredService<PostgresOptions>();

        options.DefaultConnectionString.Should().Be("Host=main;Database=sales");
        options.NamedConnections.Should().ContainKey("reporting");
    }

    [Fact]
    public void Connections_added_after_the_connector_are_kept()
    {
        var services = new ServiceCollection()
            .AddPostgresConnector(o => o.DefaultConnectionString = "Host=main;Database=sales")
            .AddPostgresConnection("reporting", "Host=reporting;Database=sales")
            .BuildServiceProvider();

        services.GetRequiredService<PostgresOptions>().NamedConnections.Should().ContainKey("reporting");
    }

    [Fact]
    public void Custom_options_take_over_connections_added_before_them()
    {
        var services = new ServiceCollection()
            .AddDefaultPostgresConnection("Host=main;Database=sales")
            .AddPostgresConnection("reporting", "Host=reporting;Database=sales")
            .AddPostgresConnector<CustomOptions>(o => o.Tag = "custom")
            .BuildServiceProvider();

        var options = services.GetRequiredService<PostgresOptions>().Should().BeOfType<CustomOptions>().Subject;

        options.Tag.Should().Be("custom");
        options.DefaultConnectionString.Should().Be("Host=main;Database=sales");
        options.NamedConnections.Should().ContainKey("reporting");
    }

    [Fact]
    public void Registers_the_node_factory_interfaces()
    {
        var services = new ServiceCollection().AddPostgresConnector(o => o.DefaultConnectionString = "Host=main;Database=sales").BuildServiceProvider();

        services.GetService<IPostgresSourceNodeFactory>().Should().NotBeNull();
        services.GetService<IPostgresSinkNodeFactory>().Should().NotBeNull();
    }

    private sealed class CustomOptions : PostgresOptions
    {
        public string? Tag { get; set; }
    }
}
