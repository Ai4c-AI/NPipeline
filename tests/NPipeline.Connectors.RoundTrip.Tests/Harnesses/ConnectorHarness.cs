using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.RoundTrip.Tests.Harnesses;

/// <summary>Writes items through a connector's sink and reads them back through its source.</summary>
public abstract class ConnectorHarness
{
    public abstract Task WriteAsync<T>(IReadOnlyList<T> items, CancellationToken cancellationToken = default)
        where T : notnull;

    public abstract Task<List<T>> ReadAsync<T>(CancellationToken cancellationToken = default);

    public async Task<List<T>> RoundTripAsync<T>(IReadOnlyList<T> items, CancellationToken cancellationToken = default)
        where T : notnull
    {
        await WriteAsync(items, cancellationToken);
        return await ReadAsync<T>(cancellationToken);
    }
}

/// <summary>A harness for a file connector, backed by <see cref="InMemoryStorageProvider" />.</summary>
public abstract class FileConnectorHarness(string extension) : ConnectorHarness
{
    public InMemoryStorageProvider Provider { get; init; } = new();

    public StorageUri Uri { get; } = InMemoryStorageProvider.Uri($"data{extension}");

    public override Task WriteAsync<T>(IReadOnlyList<T> items, CancellationToken cancellationToken = default) =>
        NodeRunner.WriteAsync(CreateSink<T>(), items, cancellationToken);

    public override Task<List<T>> ReadAsync<T>(CancellationToken cancellationToken = default) =>
        NodeRunner.ReadAsync(CreateSource<T>(), cancellationToken);

    protected abstract SinkNode<T> CreateSink<T>();

    protected abstract SourceNode<T> CreateSource<T>();
}

public static class NodeRunner
{
    public static async Task WriteAsync<T>(SinkNode<T> sink, IEnumerable<T> items, CancellationToken cancellationToken = default)
        where T : notnull
    {
        await using var input = new InMemoryDataStream<T>(items);
        await sink.ConsumeAsync(input, PipelineContext.CreateDefault(), cancellationToken);

        if (sink is IAsyncDisposable disposable)
            await disposable.DisposeAsync();
    }

    public static async Task<List<T>> ReadAsync<T>(SourceNode<T> source, CancellationToken cancellationToken = default)
    {
        var items = new List<T>();

        await foreach (var item in source.OpenStream(PipelineContext.CreateDefault(), cancellationToken).WithCancellation(cancellationToken))
        {
            items.Add(item);
        }

        if (source is IAsyncDisposable disposable)
            await disposable.DisposeAsync();

        return items;
    }
}
