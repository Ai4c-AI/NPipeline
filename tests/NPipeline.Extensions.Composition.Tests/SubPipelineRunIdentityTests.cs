using System.Collections.Concurrent;
using AwesomeAssertions;
using NPipeline.Attributes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Extensions.Composition.Tests;

/// <summary>
///     A sub-pipeline run is linked to its parent through <see cref="PipelineContext.RunIdentity" />, and the
///     <see cref="PipelineContextKeys.SubPipelineContextInitializer" /> hook reaches every nested sub-pipeline.
/// </summary>
public sealed class SubPipelineRunIdentityTests
{
    private const string MarkerKey = "testing.initializer.marker";

    [Fact]
    public async Task SubPipelineRun_IsLinkedToItsParent()
    {
        // Arrange
        IdentityRecorder.Clear();
        var runner = PipelineRunner.Create();
        await using var context = new PipelineContext();

        // Act
        await runner.RunAsync<ParentPipeline>(context);

        // Assert
        context.RunIdentity.IsNested.Should().BeFalse();
        context.RunIdentity.ParentPipelineId.Should().BeNull();

        IdentityRecorder.Snapshots.Should().HaveCount(3).And.AllSatisfy(child =>
        {
            child.IsNested.Should().BeTrue();
            child.ParentPipelineId.Should().Be(context.RunIdentity.PipelineId);
            child.ParentPipelineName.Should().Be(context.RunIdentity.PipelineName);
            child.ParentNodeId.Should().Be("composite");
            child.PipelineName.Should().Be(PipelineAttributeHelper.GetPipelineName(typeof(RecordingSubPipeline)));
            child.RunId.Should().Be(context.RunIdentity.RunId);
        });
    }

    [Fact]
    public async Task SubPipelineContextInitializer_RunsBeforeEachSubPipeline_WithoutPropertyInheritance()
    {
        // Arrange
        IdentityRecorder.Clear();
        var calls = new ConcurrentQueue<(PipelineContext Parent, PipelineContext Child)>();
        var runner = PipelineRunner.Create();
        await using var context = new PipelineContext();

        context.Properties[PipelineContextKeys.SubPipelineContextInitializer] = new Action<PipelineContext, PipelineContext>((parent, child) =>
        {
            calls.Enqueue((parent, child));
            child.Properties[MarkerKey] = "initialized";
        });

        // Act
        await runner.RunAsync<ParentPipeline>(context);

        // Assert
        calls.Should().HaveCount(3).And.AllSatisfy(call => call.Parent.Should().BeSameAs(context));
        IdentityRecorder.Markers.Should().HaveCount(3).And.OnlyContain(marker => Equals(marker, "initialized"));
    }

    [Fact]
    public async Task SubPipelineContextInitializer_ReachesNestedSubPipelines()
    {
        // Arrange
        IdentityRecorder.Clear();
        var calls = 0;
        var runner = PipelineRunner.Create();
        await using var context = new PipelineContext();

        context.Properties[PipelineContextKeys.SubPipelineContextInitializer] = new Action<PipelineContext, PipelineContext>((_, child) =>
        {
            Interlocked.Increment(ref calls);
            child.Properties[MarkerKey] = "initialized";
        });

        // Act
        await runner.RunAsync<GrandparentPipeline>(context);

        // Assert - one middle and one inner sub-pipeline per item.
        calls.Should().Be(6);
        IdentityRecorder.Markers.Should().HaveCount(3).And.OnlyContain(marker => Equals(marker, "initialized"));

        IdentityRecorder.Snapshots.Should().AllSatisfy(inner =>
        {
            inner.ParentPipelineName.Should().Be(PipelineAttributeHelper.GetPipelineName(typeof(MiddlePipeline)));
            inner.ParentPipelineId.Should().NotBe(context.RunIdentity.PipelineId);
        });
    }

    private sealed record IdentitySnapshot(
        bool IsNested,
        Guid? ParentPipelineId,
        string? ParentPipelineName,
        string? ParentNodeId,
        string? PipelineName,
        Guid RunId);

    private static class IdentityRecorder
    {
        private static ConcurrentQueue<IdentitySnapshot> _snapshots = new();
        private static ConcurrentQueue<object?> _markers = new();

        public static IReadOnlyCollection<IdentitySnapshot> Snapshots => _snapshots;

        public static IReadOnlyCollection<object?> Markers => _markers;

        public static void Clear()
        {
            _snapshots = new ConcurrentQueue<IdentitySnapshot>();
            _markers = new ConcurrentQueue<object?>();
        }

        public static void Record(PipelineContext context)
        {
            var identity = context.RunIdentity;

            _snapshots.Enqueue(new IdentitySnapshot(identity.IsNested, identity.ParentPipelineId, identity.ParentPipelineName, identity.ParentNodeId,
                identity.PipelineName, identity.RunId));

            _markers.Enqueue(context.Properties.TryGetValue(MarkerKey, out var marker) ? marker : null);
        }
    }

    private sealed class RecordingTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            IdentityRecorder.Record(context);
            return ValueTask.FromResult(input);
        }
    }

    private sealed class RecordingSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<RecordingTransform, int, int>("record");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class MiddlePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var composite = builder.AddComposite<int, int, RecordingSubPipeline>("inner", CompositeContextConfiguration.Default);
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, composite);
            builder.Connect(composite, output);
        }
    }

    private sealed class ParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<NumbersSource, int>("source");
            var composite = builder.AddComposite<int, int, RecordingSubPipeline>("composite", CompositeContextConfiguration.Default);
            var sink = builder.AddSink<DrainingSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class GrandparentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<NumbersSource, int>("source");
            var composite = builder.AddComposite<int, int, MiddlePipeline>("middle", CompositeContextConfiguration.Default);
            var sink = builder.AddSink<DrainingSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class NumbersSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
            => new InMemoryDataStream<int>([1, 2, 3], "numbers");
    }

    private sealed class DrainingSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }
}
