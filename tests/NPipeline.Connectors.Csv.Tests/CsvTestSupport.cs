using System.Text;
using NPipeline.Connectors.Attributes;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.Csv.Tests;

/// <summary>An in-memory store and helpers to run CSV nodes against it.</summary>
public abstract class CsvTestBase
{
    protected InMemoryStorageProvider Provider { get; } = new();

    protected static StorageUri Uri(string path = "data.csv") => InMemoryStorageProvider.Uri(path);

    protected void Put(string text, string path = "data.csv") => Provider.Put(Uri(path), Encoding.UTF8.GetBytes(text));

    protected string Text(string path = "data.csv") => Encoding.UTF8.GetString(Provider.Get(Uri(path)));

    protected CsvSourceNode<T> Source<T>(Func<CsvReadOptions, CsvReadOptions>? configure = null, string path = "data.csv") =>
        CsvConnector.Source<T>(Uri(path), o => (configure ?? (x => x))(o with { Provider = Provider }));

    protected CsvSinkNode<T> Sink<T>(Func<CsvWriteOptions, CsvWriteOptions>? configure = null, string path = "data.csv") =>
        CsvConnector.Sink<T>(Uri(path), o => (configure ?? (x => x))(o with { Provider = Provider }));

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

public sealed class Person
{
    public int Id { get; set; }

    public string FirstName { get; set; } = string.Empty;

    public decimal Amount { get; set; }

    public string Country { get; set; } = "AU";
}

public sealed record PositionalPerson(int Id, string FirstName);

public sealed class AttributedPerson
{
    [Column("person_id")]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    [IgnoreColumn]
    public string Secret { get; set; } = "unset";
}

public sealed class RequiredPerson
{
    public required int Id { get; init; }

    public string? Name { get; init; }
}

public sealed class NullableRow
{
    public int Id { get; set; }

    public int? Score { get; set; }

    public DateTime? Seen { get; set; }
}

public sealed class TypedRow
{
    public DateTimeOffset At { get; set; }

    public DateOnly Day { get; set; }

    public TimeOnly Time { get; set; }

    public TimeSpan Span { get; set; }

    public Guid Key { get; set; }

    public DayOfWeek Weekday { get; set; }

    public double Ratio { get; set; }

    public bool Flag { get; set; }
}

public sealed class NestedRow
{
    public int Id { get; set; }

    public List<string> Tags { get; set; } = [];
}
