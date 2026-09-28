using System.Net;
using System.Text.Json;
using System.Web;
using BenchmarkDotNet.Attributes;
using NPipeline.Connectors.Http;
using NPipeline.Connectors.Http.Pagination;

namespace NPipeline.Connectors.Benchmarks.Benchmarks;

/// <summary>
///     HTTP source and sink against an in-process handler with no network, so the numbers measure body handling,
///     deserialisation, pagination and serialisation.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 2, iterationCount: 8)]
public class HttpBenchmarks : IDisposable
{
    private static readonly Uri BaseUri = new("https://api.bench/items");

    private PagedHandler _source = null!;

    private PagedHandler _wrappedSource = null!;

    private WideRecord[] _records = [];

    private const int Rows = 100_000;

    private const int PageSize = 1_000;

    [GlobalSetup]
    public void Setup()
    {
        _records = WideRecord.Generate(Rows);
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);

        // Pre-serialised pages, so the handler costs the same for every implementation of the source.
        var pages = _records.Chunk(PageSize)
            .Select(page => JsonSerializer.SerializeToUtf8Bytes(page, options))
            .ToArray();

        _source = new PagedHandler(pages);

        // The common API shape: items under a property, with metadata around them.
        _wrappedSource = new PagedHandler(_records.Chunk(PageSize)
            .Select(page => JsonSerializer.SerializeToUtf8Bytes(new { meta = new { total = Rows }, data = page }, options))
            .ToArray());

        foreach (var read in new[] { SourcePaged().GetAwaiter().GetResult(), SourcePagedWrapped().GetAwaiter().GetResult() })
        {
            if (read != Rows)
                throw new InvalidOperationException($"{nameof(HttpBenchmarks)} read {read} of {Rows} rows during setup.");
        }
    }

    public void Dispose()
    {
        _source.Dispose();
        _wrappedSource.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>Page-number pagination over root-array pages.</summary>
    [Benchmark]
    public Task<int> SourcePaged()
    {
        var source = HttpConnector.Source<WideRecord>(BaseUri, new HttpClient(_source, false), o => o with
        {
            Pagination = HttpPagination.PageNumber(new PageNumberPaginationOptions { PageSize = PageSize }),
        });

        return NodeRunner.ReadAsync(source);
    }

    /// <summary>Page-number pagination over <c>{"meta":{"total":N},"data":[…]}</c> pages, stopping at the total.</summary>
    [Benchmark]
    public Task<int> SourcePagedWrapped()
    {
        var source = HttpConnector.Source<WideRecord>(BaseUri, new HttpClient(_wrappedSource, false), o => o with
        {
            ItemsJsonPath = "data",
            Pagination = HttpPagination.PageNumber(new PageNumberPaginationOptions { PageSize = PageSize, TotalItemsJsonPath = "meta.total" }),
        });

        return NodeRunner.ReadAsync(source);
    }

    [Benchmark]
    public Task SinkBatched() =>
        NodeRunner.WriteAsync(
            HttpConnector.Sink<WideRecord>(BaseUri, new HttpClient(new AcceptingHandler(), true), o => o with { BatchSize = 100 }),
            _records);

    private sealed class PagedHandler(byte[][] pages) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var page = int.Parse(HttpUtility.ParseQueryString(request.RequestUri!.Query)["page"] ?? "1");

            var body = page <= pages.Length
                ? pages[page - 1]
                : "[]"u8.ToArray();

            var content = new ByteArrayContent(body);
            content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content });
        }
    }

    /// <summary>Drains the request body, as a server would, and accepts it.</summary>
    private sealed class AcceptingHandler : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Content is not null)
                await request.Content.CopyToAsync(Stream.Null, cancellationToken);

            return new HttpResponseMessage(HttpStatusCode.Created);
        }
    }
}
