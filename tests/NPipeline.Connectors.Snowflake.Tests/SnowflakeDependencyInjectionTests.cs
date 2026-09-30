using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.DependencyInjection;

namespace NPipeline.Connectors.Snowflake.Tests;

public sealed class SnowflakeDependencyInjectionTests
{
    [Fact]
    public void Connections_added_before_the_connector_keep_its_configuration()
    {
        var services = new ServiceCollection()
            .AddSnowflakeConnection("reporting", "account=reporting;db=SALES")
            .AddSnowflakeConnector(o => o.DefaultConnectionString = "account=main;db=SALES")
            .BuildServiceProvider();

        var options = services.GetRequiredService<SnowflakeOptions>();

        options.DefaultConnectionString.Should().Be("account=main;db=SALES");
        options.NamedConnections.Should().ContainKey("reporting");
    }

    [Fact]
    public void Connections_added_after_the_connector_are_kept()
    {
        var services = new ServiceCollection()
            .AddSnowflakeConnector(o => o.DefaultConnectionString = "account=main;db=SALES")
            .AddSnowflakeConnection("reporting", "account=reporting;db=SALES")
            .BuildServiceProvider();

        services.GetRequiredService<SnowflakeOptions>().NamedConnections.Should().ContainKey("reporting");
    }

    [Fact]
    public void Custom_options_take_over_connections_added_before_them()
    {
        var services = new ServiceCollection()
            .AddDefaultSnowflakeConnection("account=main;db=SALES")
            .AddSnowflakeConnection("reporting", "account=reporting;db=SALES")
            .AddSnowflakeConnector<CustomOptions>(o => o.Tag = "custom")
            .BuildServiceProvider();

        var options = services.GetRequiredService<SnowflakeOptions>().Should().BeOfType<CustomOptions>().Subject;

        options.Tag.Should().Be("custom");
        options.DefaultConnectionString.Should().Be("account=main;db=SALES");
        options.NamedConnections.Should().ContainKey("reporting");
    }

    [Fact]
    public void Registers_the_node_factory_interfaces()
    {
        var services = new ServiceCollection().AddSnowflakeConnector(o => o.DefaultConnectionString = "account=main;db=SALES").BuildServiceProvider();

        services.GetService<ISnowflakeSourceNodeFactory>().Should().NotBeNull();
        services.GetService<ISnowflakeSinkNodeFactory>().Should().NotBeNull();
    }

    private sealed class CustomOptions : SnowflakeOptions
    {
        public string? Tag { get; set; }
    }
}
