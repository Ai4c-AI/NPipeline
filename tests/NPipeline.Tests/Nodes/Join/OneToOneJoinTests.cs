using AwesomeAssertions;
using NPipeline.Attributes.Nodes;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Nodes.Internal;
using NPipeline.Pipeline;
using NPipeline.Tests.Reliability.Behavior;

namespace NPipeline.Tests.Nodes.Join;

/// <summary>
///     Verifies <see cref="JoinCardinality.OneToOne" />: each key joins once, matched items are released, and duplicates follow
///     <see cref="DuplicateKeyPolicy" />.
/// </summary>
public sealed class OneToOneJoinTests
{
    [Theory]
    [InlineData(JoinType.Inner)]
    [InlineData(JoinType.LeftOuter)]
    [InlineData(JoinType.RightOuter)]
    [InlineData(JoinType.FullOuter)]
    public async Task OneToOne_DistinctKeys_JoinsMatchesAndEmitsUnmatchedPerJoinType(JoinType joinType)
    {
        var node = new OneToOneJoin { JoinType = joinType };

        var results = await RunAsync(node, L(1), L(2), L(3), R(2), R(3), R(4));

        var expected = new List<Result> { Joined(2, 2), Joined(3, 3) };

        if (joinType is JoinType.LeftOuter or JoinType.FullOuter)
            expected.Add(LeftOnly(1));

        if (joinType is JoinType.RightOuter or JoinType.FullOuter)
            expected.Add(RightOnly(4));

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task OneToOne_ArrivalOrder_DoesNotChangeTheResult()
    {
        object[] leftFirst = [L(1), L(2), L(3), R(2), R(3), R(4)];
        object[] rightFirst = [R(2), R(3), R(4), L(1), L(2), L(3)];
        object[] interleaved = [R(3), L(1), R(2), L(3), R(4), L(2)];

        Result[] expected = [Joined(2, 2), Joined(3, 3), LeftOnly(1), RightOnly(4)];

        foreach (var order in new[] { leftFirst, rightFirst, interleaved })
        {
            var results = await RunAsync(new OneToOneJoin { JoinType = JoinType.FullOuter }, order);
            results.Should().BeEquivalentTo(expected);
        }
    }

    [Fact]
    public async Task OneToOne_DuplicateAfterMatch_Drop_IsDiscarded()
    {
        var node = new OneToOneJoin { JoinType = JoinType.FullOuter };

        var results = await RunAsync(node, L(1), R(1), R(1, "dup"), L(1, "dup"));

        results.Should().Equal(Joined(1, 1));
    }

    [Fact]
    public async Task OneToOne_DuplicateAfterMatch_EmitAsUnmatched_EmitsDuplicateOnPreservedSide()
    {
        var node = new OneToOneJoin { JoinType = JoinType.LeftOuter, DuplicateKeyPolicy = DuplicateKeyPolicy.EmitAsUnmatched };

        var results = await RunAsync(node, L(1), R(1), L(1, "dup"));

        results.Should().Equal(Joined(1, 1), LeftOnly(1, "dup"));
    }

    [Fact]
    public async Task OneToOne_DuplicateAfterMatch_EmitAsUnmatched_DiscardsDuplicateOnSideNotPreserved()
    {
        var node = new OneToOneJoin { JoinType = JoinType.LeftOuter, DuplicateKeyPolicy = DuplicateKeyPolicy.EmitAsUnmatched };

        var results = await RunAsync(node, L(1), R(1), R(1, "dup"));

        results.Should().Equal(Joined(1, 1));
    }

    [Fact]
    public async Task OneToOne_SameSideDuplicateBeforeMatch_FirstArrivalWins()
    {
        var node = new OneToOneJoin { JoinType = JoinType.LeftOuter, DuplicateKeyPolicy = DuplicateKeyPolicy.EmitAsUnmatched };

        var results = await RunAsync(node, L(1, "first"), L(1, "second"), R(1));

        results.Should().Equal(LeftOnly(1, "second"), Joined(1, 1, "first"));
    }

    [Fact]
    public async Task OneToOne_DeadLetter_SendsDuplicateWithDuplicateJoinKeyException()
    {
        var deadLetters = new CollectingDeadLetterSink();
        await using var context = new PipelineContext(new PipelineContextConfiguration(DeadLetterSink: deadLetters));
        var node = new OneToOneJoin { DuplicateKeyPolicy = DuplicateKeyPolicy.DeadLetter };
        var duplicate = R(1, "dup");

        var results = await RunAsync(node, context, L(1), R(1), duplicate);

        results.Should().Equal(Joined(1, 1));
        var envelope = deadLetters.Envelopes.Should().ContainSingle().Which;
        envelope.Item.Should().BeSameAs(duplicate);

        var error = envelope.Error.Should().BeOfType<DuplicateJoinKeyException>().Which;
        error.Key.Should().Be(1);
        error.Side.Should().Be(JoinInputSide.Right);
        error.ErrorCode.Should().Be(ErrorCodes.DuplicateJoinKey);

        // Outside a pipeline, the node's id falls back to its type name.
        envelope.Attribution.DecisionNodeId.Should().Be(nameof(OneToOneJoin));
    }

    [Fact]
    public async Task OneToOne_DeadLetterWithoutSink_FailsBeforeReadingAnyItem()
    {
        var node = new OneToOneJoin { DuplicateKeyPolicy = DuplicateKeyPolicy.DeadLetter };
        var input = new ThrowIfEnumerated();

        var act = async () => await node.ExecuteAsync(input, PipelineContext.CreateDefault());

        var thrown = await act.Should().ThrowAsync<DeadLetterSinkNotConfiguredException>();
        thrown.Which.Message.Should().Contain("DuplicateKeyPolicy = DeadLetter");
        input.Enumerated.Should().BeFalse();
    }

    [Fact]
    public async Task OneToOne_MatchedItemsAreReleased_SoCapacityIsNotUsedUp()
    {
        // With MaxCapacity = 1, a many-to-many join could retain only the first customer; one-to-one frees the slot on match.
        var node = new OneToOneJoin { JoinType = JoinType.Inner, MaxCapacity = 1 };

        var results = await RunAsync(node, L(1), R(1), L(2), R(2), L(3), R(3));

        results.Should().Equal(Joined(1, 1), Joined(2, 2), Joined(3, 3));
    }

    [Fact]
    public void JoinSide_TryTake_RemovesTheItemAndUpdatesCount()
    {
        var side = new JoinSide<int, string>();
        side.Add(1, "a", false);
        side.Add(2, "b", false);

        side.TryTake(1, out var taken).Should().BeTrue();

        taken.Should().Be("a");
        side.Count.Should().Be(1);
        side.ContainsKey(1).Should().BeFalse();
        side.TryTake(1, out _).Should().BeFalse();
        side.Unmatched().Should().Equal("b");
    }

    [Fact]
    public async Task OneToOne_MaxMatchedKeys_ForgetsOldestKeySoItsDuplicateIsTreatedAsNew()
    {
        var node = new OneToOneJoin { JoinType = JoinType.Inner, MaxMatchedKeys = 2 };

        var results = await RunAsync(node, L(1), R(1), L(2), R(2), L(3), R(3), L(1, "again"), R(1, "again"), L(3, "dup"));

        // Matching key 3 evicts key 1, so the second pair for key 1 joins; key 3 is still remembered, so its duplicate is dropped.
        results.Should().Equal(Joined(1, 1), Joined(2, 2), Joined(3, 3), Joined(1, 1, "again"));
    }

    [Fact]
    public async Task OneToOne_WithoutMaxMatchedKeys_RemembersEveryKey()
    {
        var node = new OneToOneJoin { JoinType = JoinType.Inner };

        var results = await RunAsync(node, L(1), R(1), L(2), R(2), L(3), R(3), L(1, "again"), R(1, "again"));

        results.Should().Equal(Joined(1, 1), Joined(2, 2), Joined(3, 3));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void MaxMatchedKeys_NotPositive_Throws(int value)
    {
        var node = new OneToOneJoin();

        var act = () => node.MaxMatchedKeys = value;

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task ManyToMany_WithDuplicateKeyPolicy_FailsBeforeReadingAnyItem()
    {
        var node = new OneToOneJoin { Cardinality = JoinCardinality.ManyToMany, DuplicateKeyPolicy = DuplicateKeyPolicy.EmitAsUnmatched };
        var input = new ThrowIfEnumerated();

        var act = async () => await node.ExecuteAsync(input, PipelineContext.CreateDefault());

        (await act.Should().ThrowAsync<InvalidOperationException>())
            .Which.Message.Should().Contain(ErrorCodes.JoinOptionsRequireOneToOne).And.Contain("DuplicateKeyPolicy = EmitAsUnmatched");

        input.Enumerated.Should().BeFalse();
    }

    [Fact]
    public async Task ManyToMany_WithMaxMatchedKeys_Fails()
    {
        var node = new OneToOneJoin { Cardinality = JoinCardinality.ManyToMany, MaxMatchedKeys = 10 };

        var act = async () => await node.ExecuteAsync(new ThrowIfEnumerated(), PipelineContext.CreateDefault());

        (await act.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Contain("MaxMatchedKeys = 10");
    }

    [Theory]
    [InlineData(JoinType.Inner)]
    [InlineData(JoinType.LeftOuter)]
    [InlineData(JoinType.RightOuter)]
    [InlineData(JoinType.FullOuter)]
    public async Task OneToOne_NullKeys_NeverMatchAndFollowJoinType(JoinType joinType)
    {
        var node = new NullableKeyJoin { JoinType = joinType };

        var results = await RunAsync(node,
            new NullableLeft(null, "anon-left"),
            new NullableLeft(null, "anon-left-2"),
            new NullableRight(null, "anon-right"));

        var expected = new List<Result>();

        if (joinType is JoinType.LeftOuter or JoinType.FullOuter)
            expected.AddRange([new Result(null, "anon-left", null), new Result(null, "anon-left-2", null)]);

        if (joinType is JoinType.RightOuter or JoinType.FullOuter)
            expected.Add(new Result(null, null, "anon-right"));

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task Pipeline_DeadLettersDuplicatesToTheBuilderSink_AttributedToTheJoinNode()
    {
        var deadLetters = new CollectingDeadLetterSink();
        var sink = new CollectingSink<Result>();

        await BehaviorPipeline.RunAsync(builder =>
        {
            var left = builder.AddInMemorySource("customers", [L(1), L(2)]);
            var right = builder.AddInMemorySource("orders", [R(1), R(1, "dup"), R(3)]);
            var join = builder.AddJoin<OneToOneJoin, Left, Right, Result>("join");
            var output = builder.AddSink<CollectingSink<Result>, Result>("sink");

            _ = builder.AddPreconfiguredNodeInstance(join.Id,
                    new OneToOneJoin { JoinType = JoinType.FullOuter, DuplicateKeyPolicy = DuplicateKeyPolicy.DeadLetter })
                .AddPreconfiguredNodeInstance(output.Id, sink)
                .AddDeadLetterSink(deadLetters)
                .Connect(left, join)
                .Connect(right, join)
                .Connect(join, output);
        });

        // The right input keeps its order, so the second order for customer 1 is the duplicate however the inputs interleave.
        sink.Items.Should().BeEquivalentTo([Joined(1, 1), LeftOnly(2), RightOnly(3)]);
        var envelope = deadLetters.Envelopes.Should().ContainSingle().Which;
        envelope.Item.Should().Be(R(1, "dup"));
        envelope.Attribution.DecisionNodeId.Should().Be("join");
        envelope.Attribution.OriginNodeId.Should().Be("join");
    }

    [Fact]
    public async Task Pipeline_DeadLetterWithoutSink_FailsTheRun()
    {
        var act = () => BehaviorPipeline.RunAsync(builder =>
        {
            var left = builder.AddInMemorySource("customers", [L(1)]);
            var right = builder.AddInMemorySource("orders", [R(1)]);
            var join = builder.AddJoin<OneToOneJoin, Left, Right, Result>("join");
            var output = builder.AddSink<CollectingSink<Result>, Result>("sink");

            _ = builder.AddPreconfiguredNodeInstance(join.Id, new OneToOneJoin { DuplicateKeyPolicy = DuplicateKeyPolicy.DeadLetter })
                .AddPreconfiguredNodeInstance(output.Id, new CollectingSink<Result>())
                .Connect(left, join)
                .Connect(right, join)
                .Connect(join, output);
        });

        var thrown = await act.Should().ThrowAsync<Exception>();
        Flatten(thrown.Which).OfType<DeadLetterSinkNotConfiguredException>().Should().ContainSingle()
            .Which.NodeId.Should().Be("join");
    }

    [Fact]
    public async Task SelfJoin_OneToOne_DeadLettersTheUnwrappedItem()
    {
        var deadLetters = new CollectingDeadLetterSink();
        var sink = new CollectingSink<string>();

        await BehaviorPipeline.RunAsync(builder =>
        {
            var today = builder.AddInMemorySource("today", [new Reading(1, "t1"), new Reading(1, "t1-dup"), new Reading(2, "t2")]);
            var yesterday = builder.AddInMemorySource("yesterday", [new Reading(1, "y1"), new Reading(2, "y2")]);

            var join = builder.AddSelfJoin(today, yesterday, "readings",
                (t, y) => $"{t.Value}+{y.Value}",
                r => r.SensorId,
                cardinality: JoinCardinality.OneToOne,
                duplicateKeyPolicy: DuplicateKeyPolicy.DeadLetter);

            var output = builder.AddSink<CollectingSink<string>, string>("sink");

            _ = builder.AddPreconfiguredNodeInstance(output.Id, sink)
                .AddDeadLetterSink(deadLetters)
                .Connect(join, output);
        });

        sink.Items.Should().BeEquivalentTo(["t1+y1", "t2+y2"]);
        deadLetters.Envelopes.Should().ContainSingle().Which.Item.Should().Be(new Reading(1, "t1-dup"));
    }

    private static Left L(int id, string tag = "") => new(id, tag);

    private static Right R(int id, string tag = "") => new(id, tag);

    private static Result Joined(int left, int right, string leftTag = "") => new(left, leftTag, "");

    private static Result LeftOnly(int id, string tag = "") => new(id, tag, null);

    private static Result RightOnly(int id, string tag = "") => new(id, null, tag);

    private static Task<List<Result>> RunAsync(IJoinNode node, params object[] items) => RunAsync(node, PipelineContext.CreateDefault(), items);

    private static async Task<List<Result>> RunAsync(IJoinNode node, PipelineContext context, params object[] items)
    {
        var output = await node.ExecuteAsync(items.ToAsyncEnumerable(), context);
        var results = new List<Result>();

        await foreach (var item in output)
        {
            results.Add((Result)item!);
        }

        return results;
    }

    private static IEnumerable<Exception> Flatten(Exception exception)
    {
        var pending = new Stack<Exception>([exception]);

        while (pending.Count > 0)
        {
            var current = pending.Pop();
            yield return current;

            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    pending.Push(inner);
                }
            }
            else if (current.InnerException is not null)
                pending.Push(current.InnerException);
        }
    }

    private sealed record Left(int Id, string Tag);

    private sealed record Right(int Id, string Tag);

    /// <summary>
    ///     A joined row carries the key, the left item's tag and the right item's tag; a missing side is <c>null</c>. Matched rows
    ///     record the right tag as empty so tests can tell which left item was joined.
    /// </summary>
    private sealed record Result(int? Id, string? LeftTag, string? RightTag);

    private sealed record NullableLeft(string? Key, string Tag);

    private sealed record NullableRight(string? Key, string Tag);

    private sealed record Reading(int SensorId, string Value);

    [KeySelector(typeof(Left), nameof(Left.Id))]
    [KeySelector(typeof(Right), nameof(Right.Id))]
    private sealed class OneToOneJoin : KeyedJoinNode<int, Left, Right, Result>
    {
        public OneToOneJoin()
        {
            Cardinality = JoinCardinality.OneToOne;
        }

        public override Result CreateOutput(Left item1, Right item2) => new(item1.Id, item1.Tag, "");

        public override Result CreateOutputFromLeft(Left item1) => new(item1.Id, item1.Tag, null);

        public override Result CreateOutputFromRight(Right item2) => new(item2.Id, null, item2.Tag);
    }

    [KeySelector(typeof(NullableLeft), nameof(NullableLeft.Key))]
    [KeySelector(typeof(NullableRight), nameof(NullableRight.Key))]
    private sealed class NullableKeyJoin : KeyedJoinNode<string, NullableLeft, NullableRight, Result>
    {
        public NullableKeyJoin()
        {
            Cardinality = JoinCardinality.OneToOne;
        }

        public override Result CreateOutput(NullableLeft item1, NullableRight item2) => new(null, item1.Tag, item2.Tag);

        public override Result CreateOutputFromLeft(NullableLeft item1) => new(null, item1.Tag, null);

        public override Result CreateOutputFromRight(NullableRight item2) => new(null, null, item2.Tag);
    }

    private sealed class ThrowIfEnumerated : IAsyncEnumerable<object?>
    {
        public bool Enumerated { get; private set; }

        public IAsyncEnumerator<object?> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            Enumerated = true;
            throw new InvalidOperationException("The join read its input.");
        }
    }
}
