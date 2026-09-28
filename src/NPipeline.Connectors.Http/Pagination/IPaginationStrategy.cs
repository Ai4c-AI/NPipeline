using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

namespace NPipeline.Connectors.Http.Pagination;

/// <summary>
///     How a paginated API's pages follow one another. A strategy holds only configuration and can be shared between
///     sources and runs; each run gets its own <see cref="IPaginationCursor" /> from <see cref="Start" />.
/// </summary>
public interface IPaginationStrategy
{
    /// <summary>Starts paging one run through the endpoint at <paramref name="baseUri" />.</summary>
    IPaginationCursor Start(Uri baseUri);
}

/// <summary>One run's position in a paginated API.</summary>
public interface IPaginationCursor
{
    /// <summary>The URI of the first page.</summary>
    Uri FirstPageUri { get; }

    /// <summary>Returns the next page's URI given the page just read, or <c>null</c> when it was the last.</summary>
    Uri? GetNextPageUri(HttpPageContext page);
}

/// <summary>A page the source has read, for a pagination cursor to decide what comes next. The body is parsed once, by the source.</summary>
public sealed class HttpPageContext
{
    private readonly HttpPageBody _body;

    internal HttpPageContext(Uri uri, int pageNumber, HttpResponseMessage response, HttpPageBody body, int itemCount, long totalItemCount)
    {
        Uri = uri;
        PageNumber = pageNumber;
        StatusCode = response.StatusCode;
        Headers = response.Headers;
        _body = body;
        ItemCount = itemCount;
        TotalItemCount = totalItemCount;
    }

    /// <summary>The URI the page was read from.</summary>
    public Uri Uri { get; }

    /// <summary>The page's 1-based position in the run.</summary>
    public int PageNumber { get; }

    /// <summary>The response's status code.</summary>
    public HttpStatusCode StatusCode { get; }

    /// <summary>The response's headers, such as <c>Link</c>.</summary>
    public HttpResponseHeaders Headers { get; }

    /// <summary>The response body, parsed on first use. Valid only while the cursor handles this page.</summary>
    public JsonElement Body => _body.Root;

    /// <summary>How many items the page held (at <c>ItemsJsonPath</c>, or the root array).</summary>
    public int ItemCount { get; }

    /// <summary>How many items the run has read so far, including this page's.</summary>
    public long TotalItemCount { get; }

    /// <summary>Finds the value at a dotted path in <see cref="Body" /> (<c>meta.next_cursor</c>; a leading <c>$.</c> is allowed).</summary>
    /// <remarks>Only the value is parsed, not the whole page, so this is cheaper than reading <see cref="Body" />.</remarks>
    public bool TryGetValue(string path, out JsonElement value) => _body.TryGetValue(HttpJsonPath.Parse(path), out value);

    internal bool TryGetValue(IReadOnlyList<string> path, out JsonElement value) => _body.TryGetValue(path, out value);
}
