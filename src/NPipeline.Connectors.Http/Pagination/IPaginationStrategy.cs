namespace NPipeline.Connectors.Http.Pagination;

/// <summary>Controls how subsequent pages are requested from a paginated REST API.</summary>
public interface IPaginationStrategy
{
    /// <summary>Builds the URI for the very first page.</summary>
    /// <param name="baseUri">The base URI of the endpoint, without pagination query parameters.</param>
    Uri BuildFirstPageUri(Uri baseUri);

    /// <summary>
    ///     Given the current page URI and its completed HTTP response, returns the URI for the next page,
    ///     or <c>null</c> when no further pages exist.
    /// </summary>
    /// <param name="currentUri">The URI used to fetch <paramref name="response" />.</param>
    /// <param name="response">The completed HTTP response (headers and body already read).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    ValueTask<Uri?> GetNextPageUriAsync(
        Uri currentUri,
        HttpResponseMessage response,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns the URI for the next page, given how many items the source deserialized from the current page. The
    ///     source calls this overload; strategies that stop on a short page should override it and use
    ///     <paramref name="itemCount" />, which honours <c>ItemsJsonPath</c>, rather than counting the body themselves.
    /// </summary>
    /// <param name="currentUri">The URI used to fetch <paramref name="response" />.</param>
    /// <param name="response">The completed HTTP response (headers and body already read).</param>
    /// <param name="itemCount">The number of items the source read from this page.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    ValueTask<Uri?> GetNextPageUriAsync(
        Uri currentUri,
        HttpResponseMessage response,
        int itemCount,
        CancellationToken cancellationToken = default) =>
        GetNextPageUriAsync(currentUri, response, cancellationToken);
}
