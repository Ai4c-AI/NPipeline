using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.Benchmarks;

internal static class NodeRunner
{
    public static async Task WriteAsync<T>(SinkNode<T> sink, T[] items)
        where T : notnull
    {
        await using var input = new InMemoryDataStream<T>(items);
        await sink.ConsumeAsync(input, PipelineContext.CreateDefault(), CancellationToken.None);

        if (sink is IAsyncDisposable disposable)
            await disposable.DisposeAsync();
    }

    /// <summary>Drains the source and returns the row count, so the benchmark cannot be optimised away.</summary>
    public static async Task<int> ReadAsync<T>(SourceNode<T> source)
    {
        var count = 0;

        await foreach (var _ in source.OpenStream(PipelineContext.CreateDefault(), CancellationToken.None))
        {
            count++;
        }

        if (source is IAsyncDisposable disposable)
            await disposable.DisposeAsync();

        return count;
    }
}
