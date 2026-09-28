using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Web;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Pagination;
using NPipeline.Pipeline;
using NResilience;

namespace NPipeline.Connectors.Http.Tests.Pagination;

public sealed class HttpPaginationTests
{
    private static readonly Uri Endpoint = new("https://api.test/items?filter=all");

    [Fact]
    public async Task Page_numbers_stop_at_a_short_page()
    {
        var api = new PagedApi(request => Ids(Query(request, "page") is { } page ? int.Parse(page) : 1, pageSize: 2, total: 5));

        var ids = await ReadAsync(api, o => o with { Pagination = HttpPagination.PageNumber(new PageNumberPaginationOptions { PageSize = 2 }) });

        ids.Should().Equal(1, 2, 3, 4, 5);
        api.Queries("page").Should().Equal("1", "2", "3");
        api.Queries("filter").Should().AllBe("all", "the base URI's own query is kept");
    }

    [Fact]
    public async Task Page_numbers_stop_once_the_total_is_read()
    {
        var api = new PagedApi(request => $$$"""{"data":{{{Ids(int.Parse(Query(request, "page")!), 2, 4)}}},"meta":{"total":"4"}}""");

        var ids = await ReadAsync(api, o => o with
        {
            ItemsJsonPath = "$.data",
            Pagination = HttpPagination.PageNumber(new PageNumberPaginationOptions { PageSize = 2, TotalItemsJsonPath = "meta.total" }),
        });

        ids.Should().Equal(1, 2, 3, 4);
        api.Requests.Should().HaveCount(2, "the total says page 2 was the last, although it was full");
    }

    [Fact]
    public async Task Offsets_advance_by_the_items_read()
    {
        var api = new PagedApi(request => Ids((int.Parse(Query(request, "offset")!) / 3) + 1, 3, 7));

        var ids = await ReadAsync(api, o => o with { Pagination = HttpPagination.Offset(new OffsetPaginationOptions { Limit = 3 }) });

        ids.Should().Equal(1, 2, 3, 4, 5, 6, 7);
        api.Queries("offset").Should().Equal("0", "3", "6");
        api.Queries("limit").Should().AllBe("3");
    }

    [Theory]
    [InlineData("\"c2\"", "\"c3\"")]
    [InlineData("2", "3")]
    public async Task Cursors_may_be_strings_or_numbers(string second, string third)
    {
        var api = new PagedApi(request => (Query(request, "cursor") ?? "first") switch
        {
            "first" => $$$"""{"items":[1],"meta":{"next":{{{second}}}}}""",
            "c2" or "2" => $$$"""{"items":[2],"meta":{"next":{{{third}}}}}""",
            _ => """{"items":[3],"meta":{"next":null}}""",
        });

        var ids = await ReadAsync(api, o => o with
        {
            ItemsJsonPath = "items",
            Pagination = HttpPagination.Cursor(new CursorPaginationOptions { CursorJsonPath = "$.meta.next" }),
        });

        ids.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task A_cursor_of_the_wrong_type_fails()
    {
        var api = new PagedApi(_ => """{"items":[1],"next":{"token":"x"}}""");

        var read = () => ReadAsync(api, o => o with { ItemsJsonPath = "items", Pagination = HttpPagination.Cursor(new CursorPaginationOptions { CursorJsonPath = "next" }) });

        (await read.Should().ThrowAsync<HttpSourceException>()).WithMessage("*'next' on page 1 is Object, not a string or number*");
    }

    [Fact]
    public async Task Follows_next_urls_in_the_body_absolute_or_relative()
    {
        var api = new PagedApi(request => request.RequestUri!.AbsolutePath switch
        {
            "/items" => """{"items":[1],"links":{"next":"/items/2"}}""",
            "/items/2" => """{"items":[2],"links":{"next":"https://api.test/items/3"}}""",
            _ => """{"items":[3],"links":{"next":null}}""",
        });

        var ids = await ReadAsync(api, o => o with { ItemsJsonPath = "items", Pagination = HttpPagination.NextUrl("links.next") });

        ids.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task Follows_link_headers()
    {
        var api = new PagedApi(request =>
        {
            var page = Query(request, "page") ?? "1";
            var response = Json($"[{page}]");

            if (page != "3")
                response.Headers.TryAddWithoutValidation("Link", $"<https://api.test/items?page={int.Parse(page) + 1}>; rel=\"next\", <https://api.test/items?page=1>; rel=\"first\"");

            return response;
        });

        var ids = await ReadAsync(api, o => o with { Pagination = HttpPagination.LinkHeader });

        ids.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task A_custom_strategy_sees_the_parsed_page()
    {
        var pages = new List<(int Page, int Items, long Total)>();
        var api = new PagedApi(request => Ids(int.Parse(Query(request, "p") ?? "1"), 2, 4));

        var ids = await ReadAsync(api, o => o with
        {
            Pagination = HttpPagination.Custom(
                page =>
                {
                    pages.Add((page.PageNumber, page.ItemCount, page.TotalItemCount));
                    return page.ItemCount == 2 ? new Uri($"https://api.test/items?p={page.PageNumber + 1}") : null;
                }),
        });

        ids.Should().Equal(1, 2, 3, 4);
        pages.Should().Equal((1, 2, 2L), (2, 2, 4L), (3, 0, 4L));
    }

    [Fact]
    public async Task A_custom_strategy_can_read_the_whole_body_and_values_by_path()
    {
        var api = new PagedApi(request => (Query(request, "after") ?? "0") switch
        {
            "0" => """{"results":[1,2],"paging":{"after":"2","more":true}}""",
            _ => """{"results":[3],"paging":{"after":null,"more":false}}""",
        });

        var ids = await ReadAsync(api, o => o with
        {
            ItemsJsonPath = "results",
            Pagination = HttpPagination.Custom(page =>
                page.Body.GetProperty("paging").GetProperty("more").GetBoolean() && page.TryGetValue("paging.after", out var after)
                    ? new Uri($"https://api.test/items?after={after.GetString()}")
                    : null),
        });

        ids.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task One_options_instance_pages_independently_in_concurrent_runs()
    {
        var api = new PagedApi(request => Ids(int.Parse(Query(request, "page")!), 2, 9));

        var options = new HttpSourceOptions<int>
        {
            BaseUri = Endpoint,
            Pagination = HttpPagination.PageNumber(new PageNumberPaginationOptions { PageSize = 2 }),
            Resilience = Resilience.None,
        };

        var runs = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(() => DrainAsync(api, options))));

        runs.Should().AllSatisfy(ids => ids.Should().Equal(1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    [Fact]
    public async Task Pagination_that_returns_the_same_page_fails_instead_of_looping()
    {
        var api = new PagedApi(_ => "[1]");

        var read = () => ReadAsync(api, o => o with { Pagination = HttpPagination.Custom(page => page.Uri) });

        (await read.Should().ThrowAsync<HttpSourceException>()).WithMessage("*returned the page it had just read*");
    }

    [Fact]
    public async Task MaxPages_stops_endless_pagination()
    {
        var api = new PagedApi(request => Ids(int.Parse(Query(request, "page")!), 1, int.MaxValue));

        var ids = await ReadAsync(api, o => o with { MaxPages = 3, Pagination = HttpPagination.PageNumber(new PageNumberPaginationOptions { PageSize = 1 }) });

        ids.Should().Equal(1, 2, 3);
    }

    private static string? Query(HttpRequestMessage request, string name) => HttpUtility.ParseQueryString(request.RequestUri!.Query)[name];

    private static string Ids(int page, int pageSize, int total) =>
        $"[{string.Join(',', Enumerable.Range(((page - 1) * pageSize) + 1, pageSize).Where(id => id <= total && id > 0))}]";

    private static HttpResponseMessage Json(string json) => new(HttpStatusCode.OK) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    private static Task<List<int>> ReadAsync(PagedApi api, Func<HttpSourceOptions<int>, HttpSourceOptions<int>> configure) =>
        DrainAsync(api, configure(new HttpSourceOptions<int> { BaseUri = Endpoint, Resilience = Resilience.None with { AttemptTimeout = TimeSpan.FromSeconds(10) } }));

    private static async Task<List<int>> DrainAsync(PagedApi api, HttpSourceOptions<int> options)
    {
        using var client = new HttpClient(api, false);
        var ids = new List<int>();

        await foreach (var id in HttpConnector.Source<int>(options.BaseUri, client, _ => options).OpenStream(new PipelineContext(), CancellationToken.None))
        {
            ids.Add(id);
        }

        return ids;
    }

    /// <summary>Answers every GET with the body (or response) the test computes from the request, and records the requests.</summary>
    private sealed class PagedApi : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _respond;

        public PagedApi(Func<HttpRequestMessage, string> body)
            : this(request => Json(body(request)))
        {
        }

        public PagedApi(Func<HttpRequestMessage, HttpResponseMessage> respond)
        {
            _respond = respond;
        }

        public ConcurrentQueue<Uri> Requests { get; } = new();

        public IEnumerable<string?> Queries(string name) => Requests.Select(uri => HttpUtility.ParseQueryString(uri.Query)[name]);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Enqueue(request.RequestUri!);
            return Task.FromResult(_respond(request));
        }
    }
}
