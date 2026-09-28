using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Pipeline;
using NPipeline.Tests.Reliability.Behavior;

namespace NPipeline.Tests.Core.Context;

/// <summary>
///     A caller can give a run its own run id and pipeline name, to correlate the run with its own records.
/// </summary>
public sealed class RunIdentityConfigurationTests
{
    [Fact]
    public async Task Run_WithConfiguredRunIdAndPipelineName_ReportsThem()
    {
        // Arrange
        var runId = Guid.NewGuid();
        await using var context = new PipelineContext(PipelineContextConfiguration.Default with { RunId = runId, PipelineName = "orders" });

        // Act
        await PipelineRunner.Create().RunAsync(SingleItemPipeline(), context);

        // Assert
        context.RunIdentity.RunId.Should().Be(runId);
        context.RunIdentity.PipelineName.Should().Be("orders");
    }

    [Fact]
    public async Task Run_WithoutConfiguredIdentity_GeneratesRunIdAndUsesDefinitionName()
    {
        // Arrange
        await using var context = new PipelineContext();

        // Act
        await PipelineRunner.Create().RunAsync(SingleItemPipeline(), context);

        // Assert
        context.RunIdentity.RunId.Should().NotBeEmpty();
        context.RunIdentity.PipelineName.Should().Be(nameof(BehaviorPipeline));
        context.RunIdentity.IsNested.Should().BeFalse();
    }

    [Fact]
    public void Context_WithBlankPipelineName_TreatsItAsUnset()
    {
        // Act
        var context = new PipelineContext(PipelineContextConfiguration.Default with { PipelineName = "  " });

        // Assert
        context.RunIdentity.PipelineName.Should().BeNull();
    }

    private static BehaviorPipeline SingleItemPipeline() => new(b =>
    {
        var s = b.AddSource<StreamingSource<int>, int>("source");
        var k = b.AddSink<CollectingSink<int>, int>("sink");

        _ = b.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Of([1]))
            .AddPreconfiguredNodeInstance(k.Id, new CollectingSink<int>())
            .Connect(s, k);
    });
}
