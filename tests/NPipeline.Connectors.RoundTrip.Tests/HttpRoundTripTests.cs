using System.Text.Json;
using System.Web;
using NPipeline.Connectors.Http;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Pagination;
using NPipeline.Connectors.RoundTrip.Tests.Harnesses;
using NPipeline.Connectors.RoundTrip.Tests.Models;

namespace NPipeline.Connectors.RoundTrip.Tests;

public sealed class HttpRoundTripTests
{
    private readonly HttpHarness _harness = new();

    [Fact]
    public Task Scalars() => RoundTripScenarios.Scalars(_harness);

    [Fact]
    public Task SmallAndUnsignedIntegers() => RoundTripScenarios.SmallAndUnsignedIntegers(_harness);

    [Fact]
    public Task Nullables() => RoundTripScenarios.Nullables(_harness);

    [Fact]
    public Task Enums() => RoundTripScenarios.Enums(_harness);

    [Fact]
    public Task DateTimeOffsets() => RoundTripScenarios.DateTimeOffsets(_harness);

    [Fact]
    public Task DateOnlys() => RoundTripScenarios.DateOnlys(_harness);

    [Fact]
    public Task Text() => RoundTripScenarios.Text(_harness);

    [Fact]
    public Task ControlCharacters() => RoundTripScenarios.ControlCharacters(_harness);

    [Fact]
    public Task Binary() => RoundTripScenarios.Binary(_harness);

    [Fact]
    public Task Lists() => RoundTripScenarios.Lists(_harness);

    [Fact]
    public Task Nested() => RoundTripScenarios.Nested(_harness);

    [Fact]
    public Task PositionalRecords() => RoundTripScenarios.PositionalRecords(_harness);

    [Fact]
    public Task Volume() => RoundTripScenarios.Volume(_harness);

    [Fact]
    public Task Empty() => RoundTripScenarios.Empty(_harness);

    [Fact]
    public async Task Offset_pagination_reads_every_page_of_a_wrapped_response()
    {
        ServePages(page => $$"""{"data":{{Page(page, 2, 5)}},"total":5}""");

        var rows = await ReadAsync(o => o with
        {
            ItemsJsonPath = "data",
            Pagination = HttpPagination.PageNumber(new PageNumberPaginationOptions { PageSize = 2, TotalItemsJsonPath = "total" }),
        });

        rows.Select(r => r.Id).Should().Equal(1, 2, 3, 4, 5);
    }

    [Fact]
    public async Task Concurrent_enumerations_of_one_configuration_page_independently()
    {
        ServePages(page => Page(page, 2, 5));

        var options = new HttpSourceOptions<ScalarRecord>
        {
            BaseUri = FakeJsonApi.BaseUri,
            Pagination = HttpPagination.PageNumber(new PageNumberPaginationOptions { PageSize = 2 }),
        };

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() => ReadAsync(_ => options))));

        results.Should().AllSatisfy(rows => rows.Select(r => r.Id).Should().Equal(1, 2, 3, 4, 5));
    }

    [Fact]
    public async Task Cursor_pagination_reads_every_page()
    {
        ServePages(page => $$"""{"items":{{Page(page, 2, 5)}},"next":{{(page < 3 ? $"\"c{page + 1}\"" : "null")}}}""", "cursor", cursor => cursor is null ? 1 : int.Parse(cursor[1..]));

        var rows = await ReadAsync(o => o with
        {
            ItemsJsonPath = "items",
            Pagination = HttpPagination.Cursor(new CursorPaginationOptions { CursorJsonPath = "next" }),
        });

        rows.Select(r => r.Id).Should().Equal(1, 2, 3, 4, 5);
    }

    [Fact]
    public async Task Missing_items_path_fails_instead_of_returning_no_rows()
    {
        ServePages(page => $$"""{"data":{{Page(page, 10, 3)}}}""");

        var read = () => ReadAsync(o => o with { ItemsJsonPath = "items" });

        (await read.Should().ThrowAsync<HttpSourceException>()).WithMessage("*'items' was not found*properties: data*");
    }

    [Fact]
    public async Task Batched_sink_sends_each_item_to_its_own_uri()
    {
        var sink = HttpConnector.Sink<ScalarRecord>(FakeJsonApi.BaseUri, _harness.CreateClient(), o => o with
        {
            UriFactory = item => new Uri(FakeJsonApi.BaseUri, $"/tenants/{item.Id % 2}/items"),
            BatchSize = 4,
        });

        await NodeRunner.WriteAsync(sink, Enumerable.Range(1, 4).Select(ScalarRecord.Create));

        foreach (var (_, uri, body) in _harness.Api.Requests)
        {
            var ids = JsonDocument.Parse(body!).RootElement.EnumerateArray().Select(e => e.GetProperty("id").GetInt32());
            ids.Should().AllSatisfy(id => uri.AbsolutePath.Should().Be($"/tenants/{id % 2}/items"));
        }
    }

    private static string Page(int page, int pageSize, int total) =>
        JsonSerializer.Serialize(Enumerable.Range(((page - 1) * pageSize) + 1, pageSize).Where(id => id <= total).Select(id => new { id }));

    private void ServePages(Func<int, string> body, string parameter = "page", Func<string?, int>? parse = null) =>
        _harness.Api.Responder = request =>
        {
            var value = HttpUtility.ParseQueryString(request.RequestUri!.Query)[parameter];
            var page = parse?.Invoke(value) ?? (value is null ? 1 : int.Parse(value));
            return FakeJsonApi.Json(body(page));
        };

    private Task<List<ScalarRecord>> ReadAsync(Func<HttpSourceOptions<ScalarRecord>, HttpSourceOptions<ScalarRecord>> configure) =>
        NodeRunner.ReadAsync(new HttpSourceNode<ScalarRecord>(configure(new HttpSourceOptions<ScalarRecord> { BaseUri = FakeJsonApi.BaseUri }), _harness.CreateClient()));
}
