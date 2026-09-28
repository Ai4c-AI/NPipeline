using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Metrics;
using NPipeline.Connectors.Http.Pagination;
using NPipeline.Connectors.Http.Reliability;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NResilience;

namespace NPipeline.Connectors.Http.Nodes;

/// <summary>
///     A source that reads items from a REST API, following pagination until the last page. Each page's body is read
///     once and parsed once; its items are deserialized one by one, so an item that does not convert is a row error.
/// </summary>
/// <typeparam name="T">The item type.</typeparam>
public sealed partial class HttpSourceNode<T> : SourceNode<T>, IAsyncDisposable
{
    private const int BodyExcerptBytes = 512;

    private readonly HttpClient _httpClient;
    private readonly string[]? _itemsPath;
    private readonly JsonTypeInfo<List<T>>? _listTypeInfo;
    private readonly ILogger<HttpSourceNode<T>> _logger;
    private readonly IHttpConnectorMetrics _metrics;
    private readonly HttpSourceOptions<T> _options;
    private readonly bool _ownsClient;
    private readonly ResilientHttpSender _sender;
    private readonly JsonReaderOptions _readerOptions;
    private readonly JsonTypeInfo<T> _typeInfo;

    // The node fetches one page at a time, so the retry listener reads the request in flight from here.
    private string? _currentEndpoint;

    /// <summary>Creates a source that uses a client from <paramref name="httpClientFactory" /> (<see cref="HttpSourceOptions{T}.HttpClientName" />).</summary>
    public HttpSourceNode(
        HttpSourceOptions<T> options,
        IHttpClientFactory httpClientFactory,
        IHttpConnectorMetrics? metrics = null,
        ILogger<HttpSourceNode<T>>? logger = null)
        : this(options, CreateClient(options, httpClientFactory), metrics, logger, true)
    {
    }

    /// <summary>Creates a source that uses <paramref name="httpClient" />, which the caller keeps ownership of.</summary>
    public HttpSourceNode(
        HttpSourceOptions<T> options,
        HttpClient httpClient,
        IHttpConnectorMetrics? metrics = null,
        ILogger<HttpSourceNode<T>>? logger = null)
        : this(options, httpClient, metrics, logger, false)
    {
    }

    private HttpSourceNode(
        HttpSourceOptions<T> options,
        HttpClient httpClient,
        IHttpConnectorMetrics? metrics,
        ILogger<HttpSourceNode<T>>? logger,
        bool ownsClient)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        _options = options;
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _metrics = metrics ?? NullHttpConnectorMetrics.Instance;
        _logger = logger ?? NullLogger<HttpSourceNode<T>>.Instance;
        _ownsClient = ownsClient;
        _itemsPath = options.ItemsJsonPath is null ? null : HttpJsonPath.Parse(options.ItemsJsonPath);
        _typeInfo = options.TypeInfo ?? HttpJsonDefaults.TypeInfo<T>(options.JsonOptions);

        // The serializer's own reading rules (comments, trailing commas) apply to the whole body.
        _readerOptions = new JsonReaderOptions
        {
            AllowTrailingCommas = _typeInfo.Options.AllowTrailingCommas,
            CommentHandling = _typeInfo.Options.ReadCommentHandling,
            MaxDepth = _typeInfo.Options.MaxDepth,
        };

        // Source-generated metadata may not include List<T>; then every page takes the item-by-item path.
        _listTypeInfo = _typeInfo.Options.TryGetTypeInfo(typeof(List<T>), out var listTypeInfo) ? listTypeInfo as JsonTypeInfo<List<T>> : null;

        // The source reads every page whole, so the body is read inside the attempt: a body that breaks off
        // part-way is retried like any other transient failure.
        _sender = new ResilientHttpSender(_httpClient, options.Resilience, true, _metrics, OnResilienceEvent, options.RateLimiter, options.MaxResponseBytes);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _sender.Dispose();

        if (_ownsClient)
            _httpClient.Dispose();

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        var deadLetters = OpenDeadLetterChannel(context);
        return new DataStream<T>(FetchAllPagesAsync(deadLetters, cancellationToken), $"HttpSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<T> FetchAllPagesAsync(DeadLetterChannel deadLetters, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Pagination state belongs to this run, so one options instance can serve concurrent runs.
        var cursor = _options.Pagination.Start(_options.BaseUri);
        var run = new RunState(deadLetters);
        Uri? uri = cursor.FirstPageUri;

        while (uri is not null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_options.MaxPages is { } maxPages && run.Pages >= maxPages)
            {
                LogMaxPagesReached(_logger, typeof(T).Name, maxPages);
                yield break;
            }

            var (items, next) = await ReadPageAsync(uri, cursor, run, cancellationToken).ConfigureAwait(false);

            foreach (var item in items)
            {
                yield return item;
            }

            if (next is not null && next == uri)
                throw new HttpSourceException($"Pagination returned the page it had just read ({HttpRedaction.Full(uri)}), which would never end.");

            uri = next;
        }
    }

    private async Task<(List<T> Items, Uri? Next)> ReadPageAsync(Uri uri, IPaginationCursor cursor, RunState run, CancellationToken cancellationToken)
    {
        var endpoint = HttpRedaction.Endpoint(uri);
        HttpResponseMessage response;

        try
        {
            response = await SendAsync(uri, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _metrics.RecordError(endpoint, _options.RequestMethod.Method, ex);
            throw;
        }

        using (response)
        {
            using var body = new HttpPageBody(await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false), _readerOptions);
            run.Pages++;

            // The items array is found with a forward-only reader and deserialized from its bytes in one call; the body is
            // parsed into a document only if the pagination strategy or an error message needs it.
            FindItems(body, uri, run.Pages, response);
            var source = HttpRedaction.Full(uri);
            var (items, count) = await DeserializeAsync(body, run, source, cancellationToken).ConfigureAwait(false);
            run.Items += count;
            _metrics.RecordPageFetched(endpoint, items.Count);
            ConnectorDiagnostics.RecordRowsRead("http", uri.Scheme, items.Count);
            LogPageFetched(_logger, typeof(T).Name, run.Pages, items.Count, source);

            var next = cursor.GetNextPageUri(new HttpPageContext(uri, run.Pages, response, body, count, run.Items));
            return (items, next);
        }
    }

    /// <summary>Checks that the page has an items array, failing with what the page holds when it does not.</summary>
    private void FindItems(HttpPageBody body, Uri uri, int page, HttpResponseMessage response)
    {
        bool found;

        try
        {
            found = body.TryFindArray(_itemsPath, out _);
        }
        catch (JsonException ex)
        {
            // Typically an HTML error page returned with 200.
            var excerpt = Encoding.UTF8.GetString(body.Bytes, 0, Math.Min(body.Bytes.Length, BodyExcerptBytes));

            throw new HttpSourceException(
                $"Page {page} from {HttpRedaction.Full(uri)} is not JSON ({response.Content.Headers.ContentType?.ToString() ?? "no content type"}): {ex.Message} " +
                $"The body starts: {excerpt}", ex);
        }

        if (!found)
            throw DescribeMissingItems(body.Root, uri, page);
    }

    /// <summary>
    ///     Deserializes a page's items. The whole array goes in one call, which is much cheaper per item; only when that
    ///     fails is the page read again item by item, so the bad items become row errors and the good ones are kept.
    /// </summary>
    private async ValueTask<(List<T> Items, int Count)> DeserializeAsync(HttpPageBody body, RunState run, string source, CancellationToken cancellationToken)
    {
        if (_listTypeInfo is not null && DeserializeAll(body) is { } all)
        {
            var count = all.Count;
            run.Records += count;

            // Null elements are not items, as on the item-by-item path.
            _ = all.RemoveAll(static item => item is null);
            return (all, count);
        }

        _ = body.TryFindArray(_itemsPath, out var reader);
        var elements = HttpPageBody.FindElements(reader);
        var items = new List<T>(elements.Count);

        foreach (var element in elements)
        {
            run.Records++;
            T? item = default;
            JsonException? error = null;

            try
            {
                item = JsonSerializer.Deserialize(body.Bytes.AsSpan(element), _typeInfo);
            }
            catch (JsonException ex)
            {
                error = ex;
            }

            if (error is not null)
                await HandleRowErrorAsync(run, source, error, body.Bytes, element, cancellationToken).ConfigureAwait(false);
            else if (item is not null)
                items.Add(item);
        }

        return (items, elements.Count);
    }

    /// <summary>The page's items in one call, or <c>null</c> when one of them does not convert.</summary>
    private List<T>? DeserializeAll(HttpPageBody body)
    {
        _ = body.TryFindArray(_itemsPath, out var reader);

        try
        {
            return JsonSerializer.Deserialize(body.ArrayBytes(reader, _itemsPath is null or { Length: 0 }), _listTypeInfo!);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private HttpSourceException DescribeMissingItems(JsonElement root, Uri uri, int page)
    {
        if (_itemsPath is null)
        {
            return new HttpSourceException(
                $"Page {page} from {HttpRedaction.Full(uri)} is {Describe(root.ValueKind)}, not an array. Set ItemsJsonPath to the items array: " +
                $"{HttpJsonPath.Describe(root, [])}.");
        }

        return HttpJsonPath.TryResolve(root, _itemsPath, out var found)
            ? new HttpSourceException(
                $"ItemsJsonPath '{_options.ItemsJsonPath}' on page {page} from {HttpRedaction.Full(uri)} is {Describe(found.ValueKind)}, not an array.")
            : new HttpSourceException(
                $"ItemsJsonPath '{_options.ItemsJsonPath}' was not found on page {page} from {HttpRedaction.Full(uri)}; {HttpJsonPath.Describe(root, _itemsPath)}.");
    }

    private async ValueTask HandleRowErrorAsync(RunState run, string source, JsonException exception, byte[] body, Range item, CancellationToken cancellationToken)
    {
        var field = exception.Path is { Length: > 1 } path ? path : null;
        var error = new RowError(source, run.Records, field, Excerpt(body.AsSpan(item)), exception);
        var action = _options.RowErrorHandler?.Invoke(error) ?? RowErrorAction.Fail;

        ConnectorDiagnostics.RecordRowError("http", _options.BaseUri.Scheme, action.ToString().ToLowerInvariant());

        switch (action)
        {
            case RowErrorAction.Skip:
                return;
            case RowErrorAction.DeadLetter:
                await run.DeadLetters.SendAsync(new ConnectorRecordFailure(source, run.Records, field, error.RawExcerpt), exception, cancellationToken)
                    .ConfigureAwait(false);

                return;
            default:
                throw new RecordMappingException(error);
        }
    }

    private string? Excerpt(ReadOnlySpan<byte> item)
    {
        if (_options.RawExcerptLength == 0)
            return null;

        var raw = Encoding.UTF8.GetString(item[..Math.Min(item.Length, (_options.RawExcerptLength * 4) + 4)]);
        return raw.Length <= _options.RawExcerptLength ? raw : string.Concat(raw.AsSpan(0, _options.RawExcerptLength), "…");
    }

    private async Task<HttpResponseMessage> SendAsync(Uri uri, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(_options.RequestMethod, uri);

        foreach (var (key, value) in _options.Headers)
        {
            _ = request.Headers.TryAddWithoutValidation(key, value);
        }

        if (_options.RequestBodyFactory is { } bodyFactory)
            request.Content = bodyFactory(uri);

        // A source only reads, so its request is safe to repeat even when it is a POST that carries a query.
        _ = request.MarkRepeatable();

        await _options.Auth.ApplyAsync(request, cancellationToken).ConfigureAwait(false);

        if (_options.RequestCustomizer is { } customize)
            await customize(request, cancellationToken).ConfigureAwait(false);

        var described = HttpRedaction.Full(request.RequestUri ?? uri);
        LogSendingRequest(_logger, typeof(T).Name, _options.RequestMethod.Method, described);
        _currentEndpoint = HttpRedaction.Endpoint(uri);

        var response = await _sender.SendAsync(request, cancellationToken).ConfigureAwait(false);

        if (response.IsSuccessStatusCode)
            return response;

        using (response)
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

            throw new HttpRequestException(
                $"HttpSourceNode<{typeof(T).Name}>: {_options.RequestMethod.Method} {described} failed with " +
                $"{(int)response.StatusCode} {response.ReasonPhrase}. Body: {Truncate(body, BodyExcerptBytes)}",
                null,
                response.StatusCode);
        }
    }

    private void OnResilienceEvent(CallEvent callEvent)
    {
        if (callEvent.Kind != CallEventKind.Retrying || _currentEndpoint is not { } endpoint)
            return;

        _metrics.RecordRetry(endpoint, _options.RequestMethod.Method, callEvent.AttemptNumber);
        LogRetrying(_logger, typeof(T).Name, callEvent.AttemptNumber, endpoint);
    }

    private static string Describe(JsonValueKind kind) =>
        kind switch
        {
            JsonValueKind.Object => "an object",
            JsonValueKind.Array => "an array",
            JsonValueKind.String => "a string",
            JsonValueKind.Number => "a number",
            JsonValueKind.True or JsonValueKind.False => "a boolean",
            _ => "null",
        };

    private static string Truncate(string value, int maxLength) =>
        value.Length <= maxLength
            ? value
            : string.Concat(value.AsSpan(0, maxLength), "…");

    private static HttpClient CreateClient(HttpSourceOptions<T> options, IHttpClientFactory httpClientFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(httpClientFactory);

        return options.HttpClientName is { } name
            ? httpClientFactory.CreateClient(name)
            : httpClientFactory.CreateClient();
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "HttpSourceNode<{TypeName}>: reached MaxPages limit of {MaxPages}, stopping.")]
    private static partial void LogMaxPagesReached(ILogger logger, string typeName, int maxPages);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HttpSourceNode<{TypeName}>: page {Page} gave {Count} items from {Uri}.")]
    private static partial void LogPageFetched(ILogger logger, string typeName, int page, int count, string uri);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HttpSourceNode<{TypeName}>: sending {Method} {Uri}.")]
    private static partial void LogSendingRequest(ILogger logger, string typeName, string method, string uri);

    [LoggerMessage(Level = LogLevel.Warning, Message = "HttpSourceNode<{TypeName}>: attempt {Attempt} failed for {Uri}, retrying.")]
    private static partial void LogRetrying(ILogger logger, string typeName, int attempt, string uri);

    /// <summary>One run's counters and dead-letter channel.</summary>
    private sealed class RunState(DeadLetterChannel deadLetters)
    {
        public DeadLetterChannel DeadLetters { get; } = deadLetters;

        public int Pages { get; set; }

        public long Items { get; set; }

        public long Records { get; set; }
    }
}
