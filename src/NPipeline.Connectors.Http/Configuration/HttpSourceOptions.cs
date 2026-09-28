using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading.RateLimiting;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.Pagination;
using NPipeline.Connectors.Http.Reliability;
using NResilience;

namespace NPipeline.Connectors.Http.Configuration;

/// <summary>Options for an <see cref="Nodes.HttpSourceNode{T}" />. Create them with <see cref="HttpConnector" /> or directly.</summary>
/// <typeparam name="T">The item type.</typeparam>
public sealed record HttpSourceOptions<T>
{
    /// <summary>The default length of <see cref="RowError.RawExcerpt" />, 256 characters.</summary>
    public const int DefaultRawExcerptLength = 256;

    /// <summary>The endpoint's URI, without pagination query parameters.</summary>
    public required Uri BaseUri { get; init; }

    /// <summary>The request method. Defaults to GET; use POST for APIs that take the query in a body (<see cref="RequestBodyFactory" />).</summary>
    public HttpMethod RequestMethod { get; init; } = HttpMethod.Get;

    /// <summary>Headers sent with every request.</summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>();

    /// <summary>Builds the request body for a page, given the page's URI; <c>null</c> sends no body.</summary>
    public Func<Uri, HttpContent?>? RequestBodyFactory { get; init; }

    /// <summary>The named client to create from an <see cref="IHttpClientFactory" />; <c>null</c> uses the default client.</summary>
    public string? HttpClientName { get; init; }

    /// <summary>
    ///     The path to the items array in each response, as property names separated by dots (<c>data.items</c>; a leading
    ///     <c>$.</c> is allowed). Property names match exactly first, then case-insensitively. When <c>null</c>, the
    ///     response's root must be the array. A path that is missing, or that is not an array, fails the read.
    /// </summary>
    public string? ItemsJsonPath { get; init; }

    /// <summary>Serializer options for items. When <c>null</c>, System.Text.Json's web defaults (camelCase, case-insensitive).</summary>
    public JsonSerializerOptions? JsonOptions { get; init; }

    /// <summary>Metadata to deserialize items with, such as one from a source-generated <c>JsonSerializerContext</c>. Takes precedence over <see cref="JsonOptions" />.</summary>
    public JsonTypeInfo<T>? TypeInfo { get; init; }

    /// <summary>Authentication. Defaults to none.</summary>
    public IHttpAuthProvider Auth { get; init; } = NullAuthProvider.Instance;

    /// <summary>How pages follow one another. Defaults to <see cref="HttpPagination.None" />, a single request.</summary>
    public IPaginationStrategy Pagination { get; init; } = HttpPagination.None;

    /// <summary>Throttles requests. A lease is acquired for every attempt, including retries. Defaults to none.</summary>
    public RateLimiter? RateLimiter { get; init; }

    /// <summary>
    ///     How each request is retried and timed out. Defaults to <see cref="HttpConnectorResilience.Default" />: four
    ///     attempts, a 30-second timeout on each, and exponential backoff that honors <c>Retry-After</c>.
    /// </summary>
    public Resilience Resilience { get; init; } = HttpConnectorResilience.Default;

    /// <summary>Changes each request just before it is sent, including retries: correlation ids, tenant headers.</summary>
    public Func<HttpRequestMessage, CancellationToken, ValueTask>? RequestCustomizer { get; init; }

    /// <summary>The most pages to fetch in one run, as a guard against endless pagination. <c>null</c> for no limit.</summary>
    public int? MaxPages { get; init; }

    /// <summary>
    ///     The largest response body to accept, in bytes. It is enforced while the body is read, before it is buffered, and
    ///     a larger response fails the read without a retry. <c>null</c> for no limit.
    /// </summary>
    public long? MaxResponseBytes { get; init; }

    /// <summary>What to do with an item that fails to deserialize. Without a handler, the read fails.</summary>
    public RowErrorHandler? RowErrorHandler { get; init; }

    /// <summary>The longest raw excerpt a <see cref="RowError" /> carries, in characters; <c>0</c> omits it.</summary>
    public int RawExcerptLength { get; init; } = DefaultRawExcerptLength;

    /// <summary>Checks the options. Nodes call it from their constructors.</summary>
    /// <exception cref="ArgumentException">An option is invalid.</exception>
    public void Validate()
    {
        ArgumentNullException.ThrowIfNull(BaseUri, nameof(BaseUri));

        if (!BaseUri.IsAbsoluteUri)
            throw new ArgumentException("BaseUri must be an absolute URI.", nameof(BaseUri));

        ArgumentNullException.ThrowIfNull(RequestMethod, nameof(RequestMethod));
        ArgumentNullException.ThrowIfNull(Headers, nameof(Headers));
        ArgumentNullException.ThrowIfNull(Auth, nameof(Auth));
        ArgumentNullException.ThrowIfNull(Pagination, nameof(Pagination));
        ArgumentNullException.ThrowIfNull(Resilience, nameof(Resilience));
        Resilience.Validate();

        if (MaxPages is <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxPages), MaxPages, "MaxPages must be greater than zero.");

        if (MaxResponseBytes is <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxResponseBytes), MaxResponseBytes, "MaxResponseBytes must be greater than zero.");

        ArgumentOutOfRangeException.ThrowIfNegative(RawExcerptLength, nameof(RawExcerptLength));
    }
}
