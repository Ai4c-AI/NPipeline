using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.DependencyInjection;

namespace NPipeline.Connectors.MySql.Tests;

public sealed class MySqlDependencyInjectionTests
{
    [Fact]
    public void Connections_added_before_the_connector_keep_its_configuration()
    {
        var services = new ServiceCollection()
            .AddMySqlConnection("reporting", "Server=reporting;Database=sales")
            .AddMySqlConnector(o => o.DefaultConnectionString = "Server=main;Database=sales")
            .BuildServiceProvider();

        var options = services.GetRequiredService<MySqlOptions>();

        options.DefaultConnectionString.Should().Be("Server=main;Database=sales");
        options.NamedConnections.Should().ContainKey("reporting");
    }

    [Fact]
    public void Connections_added_after_the_connector_are_kept()
    {
        var services = new ServiceCollection()
            .AddMySqlConnector(o => o.DefaultConnectionString = "Server=main;Database=sales")
            .AddMySqlConnection("reporting", "Server=reporting;Database=sales")
            .BuildServiceProvider();

        services.GetRequiredService<MySqlOptions>().NamedConnections.Should().ContainKey("reporting");
    }

    [Fact]
    public void Custom_options_take_over_connections_added_before_them()
    {
        var services = new ServiceCollection()
            .AddDefaultMySqlConnection("Server=main;Database=sales")
            .AddMySqlConnection("reporting", "Server=reporting;Database=sales")
            .AddMySqlConnector<CustomOptions>(o => o.Tag = "custom")
            .BuildServiceProvider();

        var options = services.GetRequiredService<MySqlOptions>().Should().BeOfType<CustomOptions>().Subject;

        options.Tag.Should().Be("custom");
        options.DefaultConnectionString.Should().Be("Server=main;Database=sales");
        options.NamedConnections.Should().ContainKey("reporting");
    }

    [Fact]
    public void Registers_the_node_factory_interfaces()
    {
        var services = new ServiceCollection().AddMySqlConnector(o => o.DefaultConnectionString = "Server=main;Database=sales").BuildServiceProvider();

        services.GetService<IMySqlSourceNodeFactory>().Should().NotBeNull();
        services.GetService<IMySqlSinkNodeFactory>().Should().NotBeNull();
    }

    private sealed class CustomOptions : MySqlOptions
    {
        public string? Tag { get; set; }
    }
}
