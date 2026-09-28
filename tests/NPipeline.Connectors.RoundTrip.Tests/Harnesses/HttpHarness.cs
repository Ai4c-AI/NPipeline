using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using NPipeline.Connectors.Http;

namespace NPipeline.Connectors.RoundTrip.Tests.Harnesses;

/// <summary>
///     An in-process JSON API. Writes append the posted items (a single object or an array) to a store; reads return
///     the store as a root array unless a test installs its own <see cref="Responder" />.
/// </summary>
public sealed class FakeJsonApi : HttpMessageHandler
{
    public static readonly Uri BaseUri = new("https://api.test/items");

    private readonly List<JsonNode?> _items = [];

    public ConcurrentQueue<(HttpMethod Method, Uri Uri, string? Body)> Requests { get; } = new();

    /// <summary>Overrides GET handling, for pagination tests.</summary>
    public Func<HttpRequestMessage, HttpResponseMessage>? Responder { get; set; }

    public IReadOnlyList<JsonNode?> Items
    {
        get
        {
            lock (_items)
            {
                return [.. _items];
            }
        }
    }

    public static HttpResponseMessage Json(string json, HttpStatusCode status = HttpStatusCode.OK) =>
        new(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = request.Content is null
            ? null
            : await request.Content.ReadAsStringAsync(cancellationToken);

        Requests.Enqueue((request.Method, request.RequestUri!, body));

        if (request.Method == HttpMethod.Get)
        {
            if (Responder is not null)
                return Responder(request);

            lock (_items)
            {
                return Json(new JsonArray([.. _items.Select(i => i?.DeepClone())]).ToJsonString());
            }
        }

        var node = JsonNode.Parse(body ?? "null");

        lock (_items)
        {
            if (node is JsonArray array)
                _items.AddRange(array.Select(i => i?.DeepClone()));
            else
                _items.Add(node);
        }

        return new HttpResponseMessage(HttpStatusCode.Created);
    }
}

public sealed class HttpHarness : ConnectorHarness
{
    public FakeJsonApi Api { get; } = new();

    public int BatchSize { get; init; } = 50;

    public HttpClient CreateClient() => new(Api, false);

    public override async Task WriteAsync<T>(IReadOnlyList<T> items, CancellationToken cancellationToken = default)
    {
        var sink = HttpConnector.Sink<T>(FakeJsonApi.BaseUri, CreateClient(), o => o with { BatchSize = BatchSize });
        await NodeRunner.WriteAsync(sink, items, cancellationToken);
    }

    public override Task<List<T>> ReadAsync<T>(CancellationToken cancellationToken = default)
    {
        var source = HttpConnector.Source<T>(FakeJsonApi.BaseUri, CreateClient());
        return NodeRunner.ReadAsync(source, cancellationToken);
    }
}
