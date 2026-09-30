using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Threading.RateLimiting;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Metrics;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NResilience;

namespace NPipeline.Connectors.Http.Tests.Nodes;

public sealed class HttpSourceBehaviourTests
{
    private static readonly Uri Endpoint = new("https://api.test/items");

    private static readonly Resilience Retrying = HttpConnectorResilienceFast();

    [Fact]
    public async Task A_body_over_MaxResponseBytes_fails_without_a_retry_even_without_a_content_length()
    {
        var api = new Api(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new UnsizedContent(Encoding.UTF8.GetBytes($"[{new string('1', 5_000)}]")) });

        var read = () => ReadAsync<int>(api, o => o with { MaxResponseBytes = 1_000, Resilience = Retrying });

        (await read.Should().ThrowAsync<HttpResponseTooLargeException>()).Which.Limit.Should().Be(1_000);
        api.Requests.Should().Be(1, "a body over the limit is permanent");
    }

    [Fact]
    public async Task A_content_length_over_MaxResponseBytes_fails_before_the_body_is_read()
    {
        var api = new Api(_ => Json($"[{new string('1', 5_000)}]"));

        var read = () => ReadAsync<int>(api, o => o with { MaxResponseBytes = 1_000, Resilience = Retrying });

        (await read.Should().ThrowAsync<HttpResponseTooLargeException>()).WithMessage("*https://api.test/items is 5,002 bytes*");
        api.Requests.Should().Be(1);
    }

    [Fact]
    public async Task A_body_within_MaxResponseBytes_is_read()
    {
        var api = new Api(_ => Json("[1,2,3]"));

        (await ReadAsync<int>(api, o => o with { MaxResponseBytes = 7 })).Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task A_response_whose_root_is_not_an_array_fails_naming_its_properties()
    {
        var api = new Api(_ => Json("""{"data":[1],"total":1}"""));

        var read = () => ReadAsync<int>(api);

        (await read.Should().ThrowAsync<HttpSourceException>()).WithMessage("*is an object, not an array. Set ItemsJsonPath*properties: data, total*");
    }

    [Fact]
    public async Task Items_paths_match_names_ignoring_case()
    {
        var api = new Api(_ => Json("""{"Result":{"Items":[1,2]}}"""));

        (await ReadAsync<int>(api, o => o with { ItemsJsonPath = "result.items" })).Should().Equal(1, 2);
    }

    [Fact]
    public async Task A_body_that_is_not_json_fails_with_its_start_and_content_type()
    {
        var api = new Api(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("<html>Maintenance</html>", Encoding.UTF8, "text/html") });

        var read = () => ReadAsync<int>(api);

        (await read.Should().ThrowAsync<HttpSourceException>()).WithMessage("*is not JSON (text/html*The body starts: <html>Maintenance</html>*");
    }

    [Fact]
    public async Task An_item_that_does_not_convert_fails_by_default_naming_its_position_and_path()
    {
        var api = new Api(_ => Json("""[{"id":1},{"id":"two"}]"""));

        var read = () => ReadAsync<Item>(api);

        var failure = (await read.Should().ThrowAsync<RecordMappingException>()).Which;
        failure.RecordNumber.Should().Be(2);
        failure.Field.Should().Be("$.id");
        failure.RawExcerpt.Should().Be("""{"id":"two"}""");
    }

    [Fact]
    public async Task Items_that_do_not_convert_can_be_skipped()
    {
        var api = new Api(_ => Json("""[{"id":1},{"id":"two"},{"id":3}]"""));

        var items = await ReadAsync<Item>(api, o => o with { RowErrorHandler = _ => RowErrorAction.Skip });

        items.Select(i => i.Id).Should().Equal(1, 3);
    }

    [Fact]
    public async Task Items_that_do_not_convert_can_be_dead_lettered()
    {
        using var client = new HttpClient(new Api(_ => Json("""[{"id":1},{"id":"two"}]""")), false);
        var deadLetters = new CapturingDeadLetterSink();
        var context = new PipelineContext();
        var source = HttpConnector.Source<Item>(Endpoint, client, o => o with { RowErrorHandler = _ => RowErrorAction.DeadLetter });

        await PipelineRunner.Create().RunAsync(new SourcePipeline(source, deadLetters), context);

        deadLetters.Captured.Should().ContainSingle().Which.Item.Should().Be(new ConnectorRecordFailure("https://api.test/items", 2, "$.id", """{"id":"two"}"""));
        context.GetSink<InMemorySinkNode<Item>>().Items.Select(i => i.Id).Should().Equal(1);
    }

    [Fact]
    public async Task Query_values_never_reach_metric_labels_logs_or_errors()
    {
        var api = new Api(_ => new HttpResponseMessage(HttpStatusCode.BadRequest) { Content = new StringContent("nope") });
        var metrics = new RecordingMetrics();

        var read = () => ReadAsync<int>(api, o => o with { Auth = new ApiKeyAuthProvider("api_key", "s3cret", ApiKeyLocation.QueryString) }, metrics);

        var failure = await read.Should().ThrowAsync<HttpRequestException>();
        failure.Which.Message.Should().Contain("https://api.test/items?api_key=REDACTED").And.NotContain("s3cret");
        metrics.Endpoints.Should().NotBeEmpty().And.AllBe("https://api.test/items");
        api.LastUri!.Query.Should().Contain("api_key=s3cret", "the request itself still carries the key");
    }

    [Fact]
    public async Task Traces_use_opentelemetry_names_and_redact_the_query()
    {
        var activities = new ConcurrentQueue<Activity>();

        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "NPipeline.Connectors.Http",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                // The listener is process-wide; a host no other test uses keeps parallel tests' activities out.
                if (activity.GetTagItem("server.address") as string == "traces.test")
                    activities.Enqueue(activity);
            },
        };

        ActivitySource.AddActivityListener(listener);
        var api = new Api(_ => Json("[1]"));

        _ = await ReadAsync<int>(api, o => o with { BaseUri = new Uri("https://traces.test:8443/items?cursor=abc&token=xyz") });

        var activity = activities.Should().ContainSingle().Subject;
        activity.GetTagItem("http.request.method").Should().Be("GET");
        activity.GetTagItem("url.full").Should().Be("https://traces.test:8443/items?cursor=REDACTED&token=REDACTED");
        activity.GetTagItem("server.address").Should().Be("traces.test");
        activity.GetTagItem("server.port").Should().Be(8443);
        activity.GetTagItem("http.response.status_code").Should().Be(200);
    }

    [Fact]
    public async Task Every_attempt_takes_a_rate_limiter_lease()
    {
        var attempts = 0;

        var api = new Api(_ => ++attempts < 3
            ? new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
            : Json("[1]"));

        var limiter = new CountingRateLimiter();

        (await ReadAsync<int>(api, o => o with { RateLimiter = limiter, Resilience = Retrying })).Should().Equal(1);
        limiter.Leases.Should().Be(3, "retries respect the limiter too");
    }

    private static Resilience HttpConnectorResilienceFast() =>
        NPipeline.Connectors.Http.Reliability.HttpConnectorResilience.Default with
        {
            Backoff = NPipeline.Connectors.Http.Reliability.HttpConnectorResilience.Default.Backoff with { TransientBase = TimeSpan.FromMilliseconds(1), MaximumDelay = TimeSpan.FromMilliseconds(5) },
        };

    private static HttpResponseMessage Json(string json) => new(HttpStatusCode.OK) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    private static async Task<List<T>> ReadAsync<T>(Api api, Func<HttpSourceOptions<T>, HttpSourceOptions<T>>? configure = null, IHttpConnectorMetrics? metrics = null)
    {
        using var client = new HttpClient(api, false);
        var options = (configure ?? (o => o))(new HttpSourceOptions<T> { BaseUri = Endpoint, Resilience = Resilience.None with { AttemptTimeout = TimeSpan.FromSeconds(10) } });
        var items = new List<T>();

        await foreach (var item in new HttpSourceNode<T>(options, client, metrics).OpenStream(new PipelineContext(), CancellationToken.None))
        {
            items.Add(item);
        }

        return items;
    }

    public sealed record Item(int Id);

    private sealed class Api(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public int Requests { get; private set; }

        public Uri? LastUri { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests++;
            LastUri = request.RequestUri;
            return Task.FromResult(respond(request));
        }
    }

    /// <summary>Content with no Content-Length, as a chunked response has.</summary>
    private sealed class UnsizedContent(byte[] bytes) : HttpContent
    {
        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => stream.WriteAsync(bytes).AsTask();

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }
    }

    private sealed class CountingRateLimiter : RateLimiter
    {
        private int _leases;

        public int Leases => _leases;

        public override TimeSpan? IdleDuration => null;

        public override RateLimiterStatistics? GetStatistics() => null;

        protected override RateLimitLease AttemptAcquireCore(int permitCount) => Acquire();

        protected override ValueTask<RateLimitLease> AcquireAsyncCore(int permitCount, CancellationToken cancellationToken) => ValueTask.FromResult<RateLimitLease>(Acquire());

        private Lease Acquire()
        {
            _ = Interlocked.Increment(ref _leases);
            return new Lease();
        }

        private sealed class Lease : RateLimitLease
        {
            public override bool IsAcquired => true;

            public override IEnumerable<string> MetadataNames => [];

            public override bool TryGetMetadata(string metadataName, out object? metadata)
            {
                metadata = null;
                return false;
            }
        }
    }

    private sealed class RecordingMetrics : IHttpConnectorMetrics
    {
        public List<string> Endpoints { get; } = [];

        public void RecordRequest(string endpoint, string method) => Endpoints.Add(endpoint);

        public void RecordResponse(string endpoint, string method, int statusCode, TimeSpan latency) => Endpoints.Add(endpoint);

        public void RecordRetry(string endpoint, string method, int attempt) => Endpoints.Add(endpoint);

        public void RecordRateLimitWait(string endpoint, TimeSpan waited) => Endpoints.Add(endpoint);

        public void RecordError(string endpoint, string method, Exception ex) => Endpoints.Add(endpoint);

        public void RecordPageFetched(string endpoint, int itemCount) => Endpoints.Add(endpoint);

        public void RecordSinkWritten(string endpoint, string method, int statusCode) => Endpoints.Add(endpoint);
    }

    private sealed class SourcePipeline(HttpSourceNode<Item> source, IDeadLetterSink deadLetters) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var handle = builder.AddSource(source, "api");
            builder.Connect(handle, builder.AddInMemorySink<Item>(context));
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
