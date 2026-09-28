using System.Text;
using System.Text.Json.Serialization;
using NPipeline.Connectors.Attributes;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.Json.Tests;

/// <summary>An in-memory store and helpers to run JSON nodes against it.</summary>
public abstract class JsonTestBase
{
    protected InMemoryStorageProvider Provider { get; } = new();

    protected static StorageUri Uri(string path = "data.json") => InMemoryStorageProvider.Uri(path);

    protected void Put(string text, string path = "data.json") => Provider.Put(Uri(path), Encoding.UTF8.GetBytes(text));

    protected string Text(string path = "data.json") => Encoding.UTF8.GetString(Provider.Get(Uri(path)));

    protected JsonSourceNode<T> Source<T>(Func<JsonReadOptions, JsonReadOptions>? configure = null, string path = "data.json") =>
        JsonConnector.Source<T>(Uri(path), o => (configure ?? (x => x))(o with { Provider = Provider }));

    protected JsonSinkNode<T> Sink<T>(Func<JsonWriteOptions, JsonWriteOptions>? configure = null, string path = "data.json") =>
        JsonConnector.Sink<T>(Uri(path), o => (configure ?? (x => x))(o with { Provider = Provider }));

    protected static async Task<List<T>> ReadAsync<T>(SourceNode<T> source)
    {
        var rows = new List<T>();

        await foreach (var row in source.OpenStream(new PipelineContext(), CancellationToken.None))
        {
            rows.Add(row);
        }

        return rows;
    }

    protected static async Task WriteAsync<T>(SinkNode<T> sink, params T[] items)
    {
        await using var input = new DataStream<T>(Enumerate(items), "items");
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
    }

    private static async IAsyncEnumerable<T> Enumerate<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }
}

public enum Tier
{
    Free,
    Pro,
}

public sealed class Customer
{
    public int Id { get; set; }

    public string FirstName { get; set; } = string.Empty;

    public decimal Balance { get; set; }

    public Tier Tier { get; set; }

    public DateOnly Joined { get; set; }

    public List<string> Tags { get; set; } = [];

    public Address? Address { get; set; }
}

public sealed record Address(string City, string Country);

public sealed record Point(int X, int Y);

public sealed class Attributed
{
    [Column("customer_id")]
    public int Id { get; set; }

    [Column("ignored_name")]
    [JsonPropertyName("display")]
    public string Name { get; set; } = string.Empty;

    [IgnoreColumn]
    public string Secret { get; set; } = "unset";
}

[JsonSerializable(typeof(Point))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower)]
public sealed partial class TestJsonContext : JsonSerializerContext;
