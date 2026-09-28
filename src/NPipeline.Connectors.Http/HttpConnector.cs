using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;

namespace NPipeline.Connectors.Http;

/// <summary>Creates HTTP sources and sinks.</summary>
/// <example>
///     <code>
/// var source = HttpConnector.Source&lt;Order&gt;(new Uri("https://api.example.com/orders"), httpClient, o => o with
/// {
///     ItemsJsonPath = "data",
///     Pagination = HttpPagination.Cursor(new CursorPaginationOptions { CursorJsonPath = "meta.next" }),
/// });
///     </code>
/// </example>
public static class HttpConnector
{
    /// <summary>A source reading <paramref name="baseUri" /> with <paramref name="httpClient" />, which the caller keeps ownership of.</summary>
    /// <param name="baseUri">The endpoint, without pagination query parameters.</param>
    /// <param name="httpClient">The client to send requests with.</param>
    /// <param name="configure">Adjusts the default options, typically with a <c>with</c> expression.</param>
    public static HttpSourceNode<T> Source<T>(Uri baseUri, HttpClient httpClient, Func<HttpSourceOptions<T>, HttpSourceOptions<T>>? configure = null) =>
        new(SourceOptions(baseUri, configure), httpClient);

    /// <summary>A source reading <paramref name="baseUri" /> with a client from <paramref name="httpClientFactory" />.</summary>
    /// <param name="baseUri">The endpoint, without pagination query parameters.</param>
    /// <param name="httpClientFactory">Creates the client, named by <see cref="HttpSourceOptions{T}.HttpClientName" />.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static HttpSourceNode<T> Source<T>(Uri baseUri, IHttpClientFactory httpClientFactory, Func<HttpSourceOptions<T>, HttpSourceOptions<T>>? configure = null) =>
        new(SourceOptions(baseUri, configure), httpClientFactory);

    /// <summary>A sink writing to <paramref name="uri" /> with <paramref name="httpClient" />, which the caller keeps ownership of.</summary>
    /// <param name="uri">The endpoint. Set <see cref="HttpSinkOptions{T}.UriFactory" /> for per-item endpoints.</param>
    /// <param name="httpClient">The client to send requests with.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static HttpSinkNode<T> Sink<T>(Uri uri, HttpClient httpClient, Func<HttpSinkOptions<T>, HttpSinkOptions<T>>? configure = null) =>
        new(SinkOptions(uri, configure), httpClient);

    /// <summary>A sink writing to <paramref name="uri" /> with a client from <paramref name="httpClientFactory" />.</summary>
    /// <param name="uri">The endpoint. Set <see cref="HttpSinkOptions{T}.UriFactory" /> for per-item endpoints.</param>
    /// <param name="httpClientFactory">Creates the client, named by <see cref="HttpSinkOptions{T}.HttpClientName" />.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static HttpSinkNode<T> Sink<T>(Uri uri, IHttpClientFactory httpClientFactory, Func<HttpSinkOptions<T>, HttpSinkOptions<T>>? configure = null) =>
        new(SinkOptions(uri, configure), httpClientFactory);

    private static HttpSourceOptions<T> SourceOptions<T>(Uri baseUri, Func<HttpSourceOptions<T>, HttpSourceOptions<T>>? configure)
    {
        var options = new HttpSourceOptions<T> { BaseUri = baseUri };
        return configure is null ? options : configure(options);
    }

    private static HttpSinkOptions<T> SinkOptions<T>(Uri uri, Func<HttpSinkOptions<T>, HttpSinkOptions<T>>? configure)
    {
        var options = new HttpSinkOptions<T> { Uri = uri };
        return configure is null ? options : configure(options);
    }
}
