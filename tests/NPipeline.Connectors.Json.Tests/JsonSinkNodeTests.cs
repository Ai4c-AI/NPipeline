using System.IO.Compression;
using System.Text;
using System.Text.Json;
using AwesomeAssertions;
using NPipeline.Connectors.Files;

namespace NPipeline.Connectors.Json.Tests;

public sealed class JsonSinkNodeTests : JsonTestBase
{
    [Fact]
    public async Task Writes_an_array_in_camel_case_with_enums_as_names_and_nested_values()
    {
        var customer = new Customer
        {
            Id = 1, FirstName = "Ada", Balance = 12.5m, Tier = Tier.Pro, Joined = new DateOnly(2026, 1, 2), Tags = ["a"],
            Address = new Address("London", "UK"),
        };

        await WriteAsync(Sink<Customer>(), customer);

        Text().Should().Be("""[{"id":1,"firstName":"Ada","balance":12.5,"tier":"Pro","joined":"2026-01-02","tags":["a"],"address":{"city":"London","country":"UK"}}]""");
        (await ReadAsync(Source<Customer>())).Single().Should().BeEquivalentTo(customer);
    }

    [Fact]
    public async Task Writes_ndjson_for_ndjson_and_jsonl_files()
    {
        await WriteAsync(Sink<Point>(path: "points.ndjson"), new Point(1, 2), new Point(3, 4));
        await WriteAsync(Sink<Point>(path: "points.jsonl"), new Point(5, 6));

        Text("points.ndjson").Should().Be("{\"x\":1,\"y\":2}\n{\"x\":3,\"y\":4}\n");
        Text("points.jsonl").Should().Be("{\"x\":5,\"y\":6}\n");
    }

    [Fact]
    public async Task The_format_can_be_forced()
    {
        await WriteAsync(Sink<Point>(o => o with { Format = JsonFormat.NewlineDelimited }), new Point(1, 2));
        await WriteAsync(Sink<Point>(o => o with { Format = JsonFormat.Array }, "points.ndjson"), new Point(1, 2));

        Text().Should().Be("{\"x\":1,\"y\":2}\n");
        Text("points.ndjson").Should().Be("""[{"x":1,"y":2}]""");
    }

    [Fact]
    public async Task Indents_arrays_but_never_ndjson()
    {
        await WriteAsync(Sink<Point>(o => o with { WriteIndented = true }), new Point(1, 2));
        await WriteAsync(Sink<Point>(o => o with { WriteIndented = true }, "points.ndjson"), new Point(1, 2));

        Text().ReplaceLineEndings("\n").Should().Be("[\n  {\n    \"x\": 1,\n    \"y\": 2\n  }\n]");
        Text("points.ndjson").Should().Be("{\"x\":1,\"y\":2}\n");
    }

    [Fact]
    public async Task An_empty_input_is_an_empty_array()
    {
        await WriteAsync(Sink<Point>());

        Text().Should().Be("[]");
    }

    [Fact]
    public async Task Null_items_fail_by_default_and_can_be_written_or_skipped()
    {
        var fail = () => WriteAsync(Sink<Point?>(), new Point(1, 1), null);

        await fail.Should().ThrowAsync<InvalidOperationException>();
        await WriteAsync(Sink<Point?>(o => o with { NullItems = NullItemHandling.Write }), new Point(1, 1), null);
        await WriteAsync(Sink<Point?>(o => o with { NullItems = NullItemHandling.Skip }, "skipped.json"), null, new Point(2, 2));

        Text().Should().Be("""[{"x":1,"y":1},null]""");
        Text("skipped.json").Should().Be("""[{"x":2,"y":2}]""");
    }

    [Fact]
    public async Task Honours_column_and_ignore_attributes()
    {
        await WriteAsync(Sink<Attributed>(), new Attributed { Id = 7, Name = "Ada" });

        Text().Should().Be("""[{"customer_id":7,"display":"Ada"}]""");
    }

    [Fact]
    public async Task Uses_caller_serializer_options()
    {
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.KebabCaseLower };

        await WriteAsync(Sink<Customer>(o => o with { SerializerOptions = options }), new Customer { Id = 1, FirstName = "Ada" });

        Text().Should().StartWith("""[{"id":1,"first-name":"Ada",""");
    }

    [Fact]
    public async Task Writes_with_source_generated_metadata()
    {
        var sink = JsonConnector.Sink(Uri(), TestJsonContext.Default.Point, o => o with { Provider = Provider });

        await WriteAsync(sink, new Point(1, 2));

        Text().Should().Be("""[{"x":1,"y":2}]""");
    }

    [Fact]
    public async Task Compresses_by_suffix_and_picks_the_format_underneath()
    {
        await WriteAsync(Sink<Point>(path: "points.ndjson.gz"), new Point(1, 2));

        await using var zip = new GZipStream(new MemoryStream(Provider.Get(Uri("points.ndjson.gz"))), CompressionMode.Decompress);
        using var reader = new StreamReader(zip, Encoding.UTF8);
        (await reader.ReadToEndAsync()).Should().Be("{\"x\":1,\"y\":2}\n");
        (await ReadAsync(Source<Point>(path: "points.ndjson.gz"))).Should().Equal(new Point(1, 2));
    }

    [Fact]
    public async Task Writes_large_outputs_in_chunks_that_read_back_whole()
    {
        var points = Enumerable.Range(0, 50_000).Select(i => new Point(i, -i)).ToArray();

        await WriteAsync(Sink<Point>(), points);
        await WriteAsync(Sink<Point>(path: "points.ndjson"), points);

        (await ReadAsync(Source<Point>())).Should().Equal(points);
        (await ReadAsync(Source<Point>(path: "points.ndjson"))).Should().Equal(points);
    }
}
