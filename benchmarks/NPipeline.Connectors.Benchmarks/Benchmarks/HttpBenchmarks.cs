using System.Net;
using System.Text.Json;
using System.Web;
using BenchmarkDotNet.Attributes;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
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

        var read = SourcePaged().GetAwaiter().GetResult();

        if (read != Rows)
            throw new InvalidOperationException($"{nameof(HttpBenchmarks)} read {read} of {Rows} rows during setup.");
    }

    /// <summary>
    ///     Offset pagination over root-array pages. Pages are root arrays because HTTP-1 stops the source after the
    ///     first page of any wrapped (<c>{"data": [...]}</c>) response; switch to a wrapped shape once it is fixed.
    /// </summary>
    public void Dispose()
    {
        _source.Dispose();
        GC.SuppressFinalize(this);
    }

    [Benchmark]
    public Task<int> SourcePaged()
    {
        var configuration = new HttpSourceConfiguration
        {
            BaseUri = BaseUri,
            Pagination = new OffsetPaginationStrategy(new OffsetPaginationOptions { PageSize = PageSize }),
        };

        return NodeRunner.ReadAsync(new HttpSourceNode<WideRecord>(configuration, new HttpClient(_source, false)));
    }

    [Benchmark]
    public Task SinkBatched() =>
        NodeRunner.WriteAsync(
            new HttpSinkNode<WideRecord>(new HttpSinkConfiguration { Uri = BaseUri, BatchSize = 100 }, new HttpClient(new AcceptingHandler(), true)),
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
