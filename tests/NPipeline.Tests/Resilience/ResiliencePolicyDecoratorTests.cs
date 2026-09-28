using System.Collections.Concurrent;
using AwesomeAssertions;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;
using NPipeline.Tests.Reliability.Behavior;

namespace NPipeline.Tests.Resilience;

/// <summary>
///     <see cref="PipelineContextKeys.ResiliencePolicyDecorator" /> wraps the run's policy and every node's own policy,
///     so an extension can observe or adjust failure decisions without replacing the pipeline's policies.
/// </summary>
public sealed class ResiliencePolicyDecoratorTests
{
    [Fact]
    public async Task Decorator_IsAppliedToRunPolicyAndNodePolicies()
    {
        // Arrange
        var decorated = new ConcurrentQueue<(string? NodeId, IResiliencePolicy Inner)>();
        var nodePolicy = new FixedDecisionPolicy(ResilienceDecision.Skip);
        await using var context = new PipelineContext();

        context.Properties[PipelineContextKeys.ResiliencePolicyDecorator] = new Func<string?, IResiliencePolicy, IResiliencePolicy>((nodeId, inner) =>
        {
            decorated.Enqueue((nodeId, inner));
            return inner;
        });

        // Act
        await PipelineRunner.Create().RunAsync(FailingPipeline(new CollectingSink<int>(), nodePolicy), context);

        // Assert
        decorated.Should().ContainSingle(d => d.NodeId == null);
        decorated.Should().ContainSingle(d => d.NodeId == "fail").Which.Inner.Should().BeSameAs(nodePolicy);
    }

    [Fact]
    public async Task DecoratedNodePolicy_DecidesItemFailures()
    {
        // Arrange - the node's own policy fails the run; the decorator turns failures into skips.
        var sink = new CollectingSink<int>();
        await using var context = new PipelineContext();

        context.Properties[PipelineContextKeys.ResiliencePolicyDecorator] = new Func<string?, IResiliencePolicy, IResiliencePolicy>((nodeId, inner) =>
            nodeId == "fail"
                ? new FixedDecisionPolicy(ResilienceDecision.Skip)
                : inner);

        // Act
        await PipelineRunner.Create().RunAsync(FailingPipeline(sink, new FixedDecisionPolicy(ResilienceDecision.Fail)), context);

        // Assert
        sink.Items.Should().Equal(1, 3);
    }

    private static BehaviorPipeline FailingPipeline(CollectingSink<int> sink, IResiliencePolicy nodePolicy) => new(b =>
    {
        var s = b.AddSource<StreamingSource<int>, int>("source");
        var f = b.AddTransform<FailsOnTwoTransform, int, int>("fail");
        var k = b.AddSink<CollectingSink<int>, int>("sink");

        _ = b.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Of([1, 2, 3]))
            .AddPreconfiguredNodeInstance(k.Id, sink)
            .Connect(s, f)
            .Connect(f, k);

        _ = b.AddResiliencePolicy(f, nodePolicy);
    });

    private sealed class FailsOnTwoTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) =>
            item == 2
                ? throw new InvalidOperationException("boom")
                : ValueTask.FromResult(item);
    }
}
