using AwesomeAssertions;
using NPipeline.DataFlow.Routing;

namespace NPipeline.Tests.DataFlow.Routing;

public sealed class RouteOptionsTests
{
    [Fact]
    public void RouteOptions_ReadThroughIRouteOptions_ExposesConfiguration()
    {
        // Arrange
        IRouteOptions options = new RouteOptions<int>()
            .When("even", i => i % 2 == 0)
            .When("big", i => i > 100)
            .Otherwise("rest")
            .WithMatchMode(RouteMatchMode.AllMatches)
            .WithNoMatchBehavior(NoRouteMatchBehavior.Throw);

        // Assert
        options.OutputNames.Should().Equal("even", "big");
        options.OtherwiseOutputName.Should().Be("rest");
        options.MatchMode.Should().Be(RouteMatchMode.AllMatches);
        options.NoMatchBehavior.Should().Be(NoRouteMatchBehavior.Throw);
    }
}
