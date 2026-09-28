using System.IO.Compression;
using System.Text;
using System.Text.Json;
using AwesomeAssertions;
using NPipeline.Connectors.Errors;

namespace NPipeline.Connectors.Json.Tests;

public sealed class JsonSourceNodeTests : JsonTestBase
{
    private const string Ada = """{"id":1,"firstName":"Ada","balance":12.5,"tier":"Pro","joined":"2026-01-02","tags":["a","b"],"address":{"city":"London","country":"UK"}}""";

    [Fact]
    public async Task Reads_the_elements_of_a_root_array_with_nested_values()
    {
        Put($"[{Ada}]");

        var row = (await ReadAsync(Source<Customer>())).Single();

        row.Should().BeEquivalentTo(new Customer
        {
            Id = 1, FirstName = "Ada", Balance = 12.5m, Tier = Tier.Pro, Joined = new DateOnly(2026, 1, 2), Tags = ["a", "b"],
            Address = new Address("London", "UK"),
        });
    }

    [Fact]
    public async Task Matches_property_names_case_insensitively()
    {
        Put("""[{"ID":1,"FirstName":"Ada","TIER":"pro"}]""");

        var row = (await ReadAsync(Source<Customer>())).Single();

        row.Should().BeEquivalentTo(new { Id = 1, FirstName = "Ada", Tier = Tier.Pro });
    }

    [Fact]
    public async Task Reads_top_level_values_including_records_spanning_lines()
    {
        Put("{\"x\":1,\"y\":2}\n\n{\n  \"x\": 3,\n  \"y\": 4\n}\n{\"x\":5,\"y\":6}", "points.ndjson");

        var rows = await ReadAsync(Source<Point>(path: "points.ndjson"));

        rows.Should().Equal(new Point(1, 2), new Point(3, 4), new Point(5, 6));
    }

    [Fact]
    public async Task A_single_root_object_is_one_record()
    {
        Put("""{"x":1,"y":2}""");

        (await ReadAsync(Source<Point>())).Should().Equal(new Point(1, 2));
    }

    [Fact]
    public async Task Reads_an_array_nested_in_a_root_object()
    {
        var padding = new string('p', 100_000);
        Put($$"""{"meta":{"note":"{{padding}}","items":[{"x":0,"y":0}]},"Data":{"page":1,"items":[{"x":1,"y":2},{"x":3,"y":4}]},"total":2}""");

        var rows = await ReadAsync(Source<Point>(o => o with { ItemsPath = "$.data.items", BufferSize = 4096 }));

        rows.Should().Equal(new Point(1, 2), new Point(3, 4));
    }

    [Fact]
    public async Task A_wrong_items_path_fails_naming_the_properties_present()
    {
        Put("""{"data":{"page":1,"rows":[]}}""");

        var read = () => ReadAsync(Source<Point>(o => o with { ItemsPath = "data.items" }));

        (await read.Should().ThrowAsync<JsonException>()).WithMessage("*'data' has no property 'items'. Properties: page, rows.*");
    }

    [Fact]
    public async Task An_items_path_that_is_not_an_array_fails()
    {
        Put("""{"data":{"items":{"x":1}}}""");

        var read = () => ReadAsync(Source<Point>(o => o with { ItemsPath = "data.items" }));

        (await read.Should().ThrowAsync<JsonException>()).WithMessage("*'items' is an object, not an array*");
    }

    [Fact]
    public async Task Reads_records_larger_than_the_buffer()
    {
        var name = new string('n', 300_000);
        Put($$"""[{"id":1,"firstName":"{{name}}"},{"id":2,"firstName":"b"}]""");

        var rows = await ReadAsync(Source<Customer>(o => o with { BufferSize = 4096 }));

        rows.Select(r => r.FirstName.Length).Should().Equal(300_000, 1);
    }

    [Fact]
    public async Task Honours_column_and_ignore_attributes_and_json_property_names()
    {
        Put("""[{"customer_id":7,"display":"Ada","ignored_name":"no","secret":"leaked"}]""");

        var row = (await ReadAsync(Source<Attributed>())).Single();

        row.Should().BeEquivalentTo(new { Id = 7, Name = "Ada", Secret = "unset" });
    }

    [Fact]
    public async Task A_record_that_does_not_convert_fails_with_its_position_and_path()
    {
        Put("""[{"x":1,"y":2},{"x":"three","y":4}]""");

        var read = () => ReadAsync(Source<Point>());

        var failure = (await read.Should().ThrowAsync<RecordMappingException>()).Which;
        failure.RecordNumber.Should().Be(2);
        failure.Field.Should().Be("$.x");
        failure.RawExcerpt.Should().Be("""{"x":"three","y":4}""");
    }

    [Fact]
    public async Task Bad_records_inside_an_array_can_be_skipped()
    {
        Put("""[1,"x",3,{"y":1},5]""");
        var errors = new List<RowError>();

        var rows = await ReadAsync(Source<int>(o => o with { RowErrorHandler = e => { errors.Add(e); return RowErrorAction.Skip; } }));

        rows.Should().Equal(1, 3, 5);
        errors.Select(e => e.RecordNumber).Should().Equal(2, 4);
    }

    [Fact]
    public async Task A_malformed_ndjson_line_is_a_row_error_and_reading_goes_on()
    {
        Put("{\"x\":1,\"y\":1}\n{\"x\":2,,}\n{\"x\":3,\"y\":3}\n", "points.ndjson");
        var errors = new List<RowError>();

        var rows = await ReadAsync(Source<Point>(o => o with { RowErrorHandler = e => { errors.Add(e); return RowErrorAction.Skip; } }, "points.ndjson"));

        rows.Should().Equal(new Point(1, 1), new Point(3, 3));
        errors.Should().ContainSingle().Which.Should().BeEquivalentTo(new { RecordNumber = 2, RawExcerpt = "{\"x\":2,,}\n" });
    }

    [Fact]
    public async Task A_malformed_array_fails_naming_the_file()
    {
        Put("""[{"x":1,"y":1},{"x":2""");

        var read = () => ReadAsync(Source<Point>());

        (await read.Should().ThrowAsync<JsonException>()).WithMessage("'mem://test/data.json' is not valid JSON after record 1*");
    }

    [Fact]
    public async Task Empty_files_and_arrays_have_no_records()
    {
        Put("", "empty.json");
        Put(" [ ] ", "array.json");

        (await ReadAsync(Source<Point>(path: "empty.json"))).Should().BeEmpty();
        (await ReadAsync(Source<Point>(path: "array.json"))).Should().BeEmpty();
    }

    [Fact]
    public async Task Skips_a_byte_order_mark()
    {
        Provider.Put(Uri(), [.. Encoding.UTF8.GetPreamble(), .. Encoding.UTF8.GetBytes("""[{"x":1,"y":2}]""")]);

        (await ReadAsync(Source<Point>())).Should().Equal(new Point(1, 2));
    }

    [Fact]
    public async Task The_format_can_be_forced()
    {
        Put("""[{"x":1,"y":2}]""", "array.ndjson");
        Put("""{"x":1,"y":2}""", "object.json");

        var asSequence = await ReadAsync(Source<Point[]>(o => o with { Format = JsonFormat.NewlineDelimited }, "array.ndjson"));
        var asArray = () => ReadAsync(Source<Point>(o => o with { Format = JsonFormat.Array }, "object.json"));

        asSequence.Single().Should().Equal(new Point(1, 2));
        (await asArray.Should().ThrowAsync<JsonException>()).WithMessage("*Expected a JSON array*");
    }

    [Fact]
    public async Task Uses_caller_serializer_options_and_still_honours_column_attributes()
    {
        Put("""[{"customer_id":7,"display":"Ada"}]""");
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

        var row = (await ReadAsync(Source<Attributed>(o => o with { SerializerOptions = options }))).Single();

        row.Id.Should().Be(7);
        options.IsReadOnly.Should().BeFalse("the caller's options are copied, not changed");
    }

    [Fact]
    public async Task Reads_with_source_generated_metadata()
    {
        Put("""[{"x":1,"y":2}]""");

        var source = JsonConnector.Source(Uri(), TestJsonContext.Default.Point, o => o with { Provider = Provider });

        (await ReadAsync(source)).Should().Equal(new Point(1, 2));
    }

    [Fact]
    public async Task Reads_every_json_file_of_a_directory_including_compressed_ones()
    {
        Put("""[{"x":2,"y":2}]""", "in/b.json");
        Put("{\"x\":3,\"y\":3}\n", "in/c.ndjson");
        Put("not json", "in/readme.txt");

        using (var gz = new MemoryStream())
        {
            await using (var zip = new GZipStream(gz, CompressionLevel.Fastest, true))
            {
                await zip.WriteAsync(Encoding.UTF8.GetBytes("""[{"x":1,"y":1}]"""));
            }

            Provider.Put(Uri("in/a.json.gz"), gz.ToArray());
        }

        var rows = await ReadAsync(Source<Point>(path: "in/"));

        rows.Select(p => p.X).Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task A_manual_mapper_reads_properties_strictly_or_leniently()
    {
        Put($"[{Ada}]");

        var rows = await ReadAsync(JsonConnector.Source(
            Uri(),
            row => $"{row.RecordNumber}:{row.Get<string>("FIRSTNAME")}:{row.Get<string>("address.city")}:{row.Get<List<string>>("tags").Count}:" +
                   $"{row.TryGet<int>("firstName", out _)}:{row.HasProperty("missing")}",
            o => o with { Provider = Provider }));

        rows.Should().Equal("1:Ada:London:2:False:False");
    }

    [Fact]
    public async Task A_manual_mapper_failure_names_the_property()
    {
        Put("""[{"id":"x"}]""");

        var read = () => ReadAsync(JsonConnector.Source(Uri(), row => row.Get<int>("id"), o => o with { Provider = Provider }));

        (await read.Should().ThrowAsync<RecordMappingException>()).Which.Field.Should().Be("id");
    }

    [Fact]
    public void Validates_options()
    {
        var emptyPath = () => Source<Point>(o => o with { ItemsPath = "$." });
        var pathWithNdjson = () => Source<Point>(o => o with { ItemsPath = "data", Format = JsonFormat.NewlineDelimited });

        emptyPath.Should().Throw<ArgumentException>();
        pathWithNdjson.Should().Throw<ArgumentException>();
    }
}
