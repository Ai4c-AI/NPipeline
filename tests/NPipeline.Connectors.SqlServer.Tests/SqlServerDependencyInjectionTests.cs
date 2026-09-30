using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.DependencyInjection;

namespace NPipeline.Connectors.SqlServer.Tests;

public sealed class SqlServerDependencyInjectionTests
{
    [Fact]
    public void Connections_added_before_the_connector_keep_its_configuration()
    {
        var services = new ServiceCollection()
            .AddSqlServerConnection("reporting", "Server=reporting;Database=sales")
            .AddSqlServerConnector(o => o.DefaultConnectionString = "Server=main;Database=sales")
            .BuildServiceProvider();

        var options = services.GetRequiredService<SqlServerOptions>();

        options.DefaultConnectionString.Should().Be("Server=main;Database=sales");
        options.NamedConnections.Should().ContainKey("reporting");
    }

    [Fact]
    public void Connections_added_after_the_connector_are_kept()
    {
        var services = new ServiceCollection()
            .AddSqlServerConnector(o => o.DefaultConnectionString = "Server=main;Database=sales")
            .AddSqlServerConnection("reporting", "Server=reporting;Database=sales")
            .BuildServiceProvider();

        services.GetRequiredService<SqlServerOptions>().NamedConnections.Should().ContainKey("reporting");
    }

    [Fact]
    public void Custom_options_take_over_connections_added_before_them()
    {
        var services = new ServiceCollection()
            .AddDefaultSqlServerConnection("Server=main;Database=sales")
            .AddSqlServerConnection("reporting", "Server=reporting;Database=sales")
            .AddSqlServerConnector<CustomOptions>(o => o.Tag = "custom")
            .BuildServiceProvider();

        var options = services.GetRequiredService<SqlServerOptions>().Should().BeOfType<CustomOptions>().Subject;

        options.Tag.Should().Be("custom");
        options.DefaultConnectionString.Should().Be("Server=main;Database=sales");
        options.NamedConnections.Should().ContainKey("reporting");
    }

    [Fact]
    public void Registers_the_node_factory_interfaces()
    {
        var services = new ServiceCollection().AddSqlServerConnector(o => o.DefaultConnectionString = "Server=main;Database=sales").BuildServiceProvider();

        services.GetService<ISqlServerSourceNodeFactory>().Should().NotBeNull();
        services.GetService<ISqlServerSinkNodeFactory>().Should().NotBeNull();
    }

    private sealed class CustomOptions : SqlServerOptions
    {
        public string? Tag { get; set; }
    }
}
