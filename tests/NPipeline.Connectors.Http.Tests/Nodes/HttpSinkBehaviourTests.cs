using System.Collections.Concurrent;
using System.Net;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Models;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Pipeline;
using NResilience;

namespace NPipeline.Connectors.Http.Tests.Nodes;

public sealed class HttpSinkBehaviourTests
{
    private static readonly Uri Endpoint = new("https://api.test/items?tenant=acme");

    [Fact]
    public async Task Sends_one_object_per_request_by_default_and_arrays_for_batches()
    {
        var single = new Api();
        var batched = new Api();
        var wrapped = new Api();

        await WriteAsync(single, o => o, new Item(1), new Item(2));
        await WriteAsync(batched, o => o with { BatchSize = 2 }, new Item(1), new Item(2), new Item(3));
        await WriteAsync(wrapped, o => o with { BatchSize = 5, BatchWrapperKey = "items" }, new Item(1));

        single.Bodies.Should().Equal("""{"id":1}""", """{"id":2}""");
        batched.Bodies.Should().Equal("""[{"id":1},{"id":2}]""", """[{"id":3}]""");
        wrapped.Bodies.Should().Equal("""{"items":[{"id":1}]}""");
        single.ContentTypes.Should().AllBe("application/json; charset=utf-8");
    }

    [Fact]
    public async Task Routes_items_with_a_typed_uri_factory_and_keys_batches_by_their_items()
    {
        var api = new Api();

        await WriteAsync(api, o => o with
        {
            UriFactory = item => new Uri($"https://api.test/tenants/{item.Id % 2}/items"),
            BatchSize = 10,
            IdempotencyKeyFactory = items => string.Join('-', items.Select(i => i.Id)),
        }, new Item(1), new Item(3), new Item(2));

        api.Requests.Select(r => (r.Uri.AbsolutePath, r.IdempotencyKey)).Should().Equal(("/tenants/1/items", "1-3"), ("/tenants/0/items", "2"));
    }

    [Fact]
    public async Task A_failed_request_fails_the_write_by_default_without_leaking_query_values()
    {
        var api = new Api(HttpStatusCode.UnprocessableEntity);

        var write = () => WriteAsync(api, o => o, new Item(1));

        var failure = await write.Should().ThrowAsync<HttpRequestException>();
        failure.Which.StatusCode.Should().Be(HttpStatusCode.UnprocessableEntity);
        failure.Which.Message.Should().Contain("https://api.test/items?tenant=REDACTED").And.Contain("rejected");
    }

    [Fact]
    public async Task Failed_requests_can_be_skipped()
    {
        var api = new Api(HttpStatusCode.BadRequest, HttpStatusCode.Created);

        await WriteAsync(api, o => o with { FailedRequests = HttpFailedRequestAction.Skip }, new Item(1), new Item(2));

        api.Requests.Should().HaveCount(2);
    }

    [Fact]
    public async Task Failed_requests_can_be_dead_lettered_with_their_items()
    {
        var api = new Api(HttpStatusCode.BadRequest, HttpStatusCode.Created);
        using var client = new HttpClient(api, false);
        var deadLetters = new CapturingDeadLetterSink();
        var sink = HttpConnector.Sink<Item>(Endpoint, client, o => o with { FailedRequests = HttpFailedRequestAction.DeadLetter, BatchSize = 2, Resilience = Resilience.None });

        await PipelineRunner.Create().RunAsync(new SinkPipeline(sink, deadLetters), new PipelineContext());

        var envelope = deadLetters.Captured.Should().ContainSingle().Subject;
        envelope.Attribution.DecisionNodeId.Should().Be("api");
        var failure = envelope.Item.Should().BeOfType<HttpRequestFailure<Item>>().Subject;
        failure.Should().BeEquivalentTo(new { Endpoint = "https://api.test/items?tenant=REDACTED", Method = "POST", StatusCode = HttpStatusCode.BadRequest, ResponseExcerpt = "rejected" });
        failure.Items.Should().Equal(new Item(1), new Item(2));
        envelope.Error.Should().BeOfType<HttpRequestException>();
    }

    [Fact]
    public void Validates_options()
    {
        using var client = new HttpClient();

        var noUri = () => new HttpSinkNode<Item>(new HttpSinkOptions<Item>(), client);
        var badBatch = () => HttpConnector.Sink<Item>(Endpoint, client, o => o with { BatchSize = 0 });

        noUri.Should().Throw<ArgumentException>().WithParameterName("Uri");
        badBatch.Should().Throw<ArgumentOutOfRangeException>();
    }

    private static async Task WriteAsync(Api api, Func<HttpSinkOptions<Item>, HttpSinkOptions<Item>> configure, params Item[] items)
    {
        using var client = new HttpClient(api, false);
        var sink = HttpConnector.Sink<Item>(Endpoint, client, o => configure(o with { Resilience = Resilience.None with { AttemptTimeout = TimeSpan.FromSeconds(10) } }));
        await sink.ConsumeAsync(new DataStream<Item>(Enumerate(items), "items"), new PipelineContext(), CancellationToken.None);
    }

    private static async IAsyncEnumerable<Item> Enumerate(IEnumerable<Item> items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }

    public sealed record Item(int Id);

    /// <summary>Records each request; answers with the queued statuses, then 201.</summary>
    private sealed class Api(params HttpStatusCode[] statuses) : HttpMessageHandler
    {
        private readonly ConcurrentQueue<HttpStatusCode> _statuses = new(statuses);

        public List<(Uri Uri, string Body, string? IdempotencyKey, string? ContentType)> Requests { get; } = [];

        public IEnumerable<string> Bodies => Requests.Select(r => r.Body);

        public IEnumerable<string?> ContentTypes => Requests.Select(r => r.ContentType);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = await request.Content!.ReadAsStringAsync(cancellationToken);
            var key = request.Headers.TryGetValues("Idempotency-Key", out var values) ? values.Single() : null;
            Requests.Add((request.RequestUri!, body, key, request.Content.Headers.ContentType?.ToString()));

            var status = _statuses.TryDequeue(out var queued) ? queued : HttpStatusCode.Created;
            return new HttpResponseMessage(status) { Content = new StringContent((int)status >= 400 ? "rejected" : string.Empty) };
        }
    }

    private sealed class SinkPipeline(HttpSinkNode<Item> sink, IDeadLetterSink deadLetters) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource(() => new[] { new Item(1), new Item(2), new Item(3) }, "items");
            var api = builder.AddSink(sink, "api");
            builder.Connect(source, api);
            builder.AddDeadLetterSink(deadLetters);
        }
    }

    private sealed class CapturingDeadLetterSink : IDeadLetterSink
    {
        public List<DeadLetterEnvelope> Captured { get; } = [];

        public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
        {
            Captured.Add(envelope);
            return Task.CompletedTask;
        }
    }
}
