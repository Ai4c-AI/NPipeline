using AwesomeAssertions;
using FakeItEasy;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Observability;

namespace NPipeline.Tests.Core.Execution;

public sealed class NodeItemCountingDataStreamTests
{
    [Fact]
    public void Wrap_WithNullScope_ReturnsTheSameInstance()
    {
        // Arrange
        var input = new DataStream<int>(new[] { 1, 2, 3 }.ToAsyncEnumerable(), "source-stream");

        // Act
        var wrapped = NodeItemCounting.Wrap(input, NodeExecutionScopeRegistry.NullScope, NodeItemCount.Emitted, ownsScope: true);

        // Assert
        wrapped.Should().BeSameAs(input);
    }

    [Fact]
    public async Task Wrap_Emitted_CountsEachItemAndReleasesAnOwnedScopeAtTheEnd()
    {
        // Arrange
        var input = new DataStream<int>(new[] { 1, 2, 3 }.ToAsyncEnumerable(), "source-stream");
        var scope = A.Fake<IAutoObservabilityScope>();

        // Act
        var wrapped = (IDataStream<int>)NodeItemCounting.Wrap(input, scope, NodeItemCount.Emitted, ownsScope: true);
        var output = await wrapped.ToListAsync();

        // Assert
        output.Should().Equal(1, 2, 3);
        wrapped.StreamName.Should().Be("source-stream");
        A.CallTo(() => scope.IncrementEmitted()).MustHaveHappened(3, Times.Exactly);
        A.CallTo(() => scope.IncrementProcessed()).MustNotHaveHappened();
        A.CallTo(() => scope.Dispose()).MustHaveHappened();
    }

    [Fact]
    public async Task Wrap_Processed_CountsEachItemAndLeavesABorrowedScopeOpen()
    {
        // Arrange
        var input = new DataStream<int>(new[] { 1, 2, 3 }.ToAsyncEnumerable());
        var scope = A.Fake<IAutoObservabilityScope>();

        // Act
        var wrapped = (IDataStream<int>)NodeItemCounting.Wrap(input, scope, NodeItemCount.Processed, ownsScope: false);
        _ = await wrapped.ToListAsync();
        await wrapped.DisposeAsync();

        // Assert
        A.CallTo(() => scope.IncrementProcessed()).MustHaveHappened(3, Times.Exactly);
        A.CallTo(() => scope.IncrementEmitted()).MustNotHaveHappened();
        A.CallTo(() => scope.Dispose()).MustNotHaveHappened();
    }

    [Fact]
    public async Task Wrap_WhenTheInnerStreamFails_RecordsTheFailureOnAnOwnedScopeBeforeReleasingIt()
    {
        // Arrange
        var failure = new InvalidOperationException("source failed");
        var input = new DataStream<int>(ThrowAfterOne(failure));
        var scope = A.Fake<IAutoObservabilityScope>();

        // Act
        var wrapped = (IDataStream<int>)NodeItemCounting.Wrap(input, scope, NodeItemCount.Emitted, ownsScope: true);
        var act = async () => await wrapped.ToListAsync();

        // Assert
        (await act.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(failure);
        A.CallTo(() => scope.IncrementEmitted()).MustHaveHappenedOnceExactly();
        A.CallTo(() => scope.RecordFailure(failure)).MustHaveHappenedOnceExactly()
            .Then(A.CallTo(() => scope.Dispose()).MustHaveHappened());
    }

    [Fact]
    public async Task DisposeAsync_OnAnUnreadStream_ReleasesAnOwnedScopeAndDisposesTheInnerStream()
    {
        // Arrange
        var input = A.Fake<IDataStream<int>>();
        A.CallTo(() => input.GetDataType()).Returns(typeof(int));
        var scope = A.Fake<IAutoObservabilityScope>();
        var wrapped = NodeItemCounting.Wrap(input, scope, NodeItemCount.Emitted, ownsScope: true);

        // Act
        await wrapped.DisposeAsync();
        await wrapped.DisposeAsync();

        // Assert
        A.CallTo(() => scope.Dispose()).MustHaveHappenedOnceExactly();
        A.CallTo(() => input.DisposeAsync()).MustHaveHappenedOnceExactly();
    }

    private static async IAsyncEnumerable<int> ThrowAfterOne(Exception failure)
    {
        yield return 1;
        await Task.Yield();
        throw failure;
    }
}
