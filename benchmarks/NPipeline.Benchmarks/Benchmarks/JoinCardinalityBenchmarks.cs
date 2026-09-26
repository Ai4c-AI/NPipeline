using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using NPipeline.Attributes.Nodes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Benchmarks.Benchmarks;

/// <summary>
///     Compares a many-to-many keyed join with a one-to-one join on a stream where every key occurs once per input. The
///     one-to-one join releases each pair as it matches, so its allocated and retained memory stays flat as the stream grows.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class JoinCardinalityBenchmarks
{
    private object[] _items = [];

    [Params(100_000, 1_000_000)]
    public int Keys { get; set; }

    /// <summary>
    ///     How far the right input lags the left, in keys: the number of items a one-to-one join holds at once.
    /// </summary>
    [Params(1, 1_000)]
    public int Lag { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _items = new object[Keys * 2];
        var index = 0;

        for (var i = 0; i < Keys + Lag; i++)
        {
            if (i < Keys)
                _items[index++] = new Order(i);

            if (i >= Lag)
                _items[index++] = new Payment(i - Lag);
        }
    }

    [Benchmark(Baseline = true, Description = "ManyToMany")]
    public Task<long> ManyToMany() => RunAsync(new OrderPaymentJoin());

    [Benchmark(Description = "OneToOne")]
    public Task<long> OneToOne() => RunAsync(new OrderPaymentJoin { Cardinality = JoinCardinality.OneToOne });

    private async Task<long> RunAsync(OrderPaymentJoin join)
    {
        var output = await join.ExecuteAsync(ToAsync(_items), PipelineContext.CreateDefault());
        long sum = 0;

        await foreach (var item in output)
        {
            sum += (int)item!;
        }

        return sum;
    }

    private static async IAsyncEnumerable<object?> ToAsync(object[] items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask;
    }

    private sealed record Order(int Id);

    private sealed record Payment(int OrderId);

    [KeySelector(typeof(Order), nameof(Order.Id))]
    [KeySelector(typeof(Payment), nameof(Payment.OrderId))]
    private sealed class OrderPaymentJoin : KeyedJoinNode<int, Order, Payment, int>
    {
        public override int CreateOutput(Order item1, Payment item2) => item1.Id;
    }
}
