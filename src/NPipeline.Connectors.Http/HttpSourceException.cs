namespace NPipeline.Connectors.Http;

/// <summary>
///     An HTTP source read a response it could not use: a body that is not JSON, an items path that is missing or not an
///     array, or pagination that cannot continue. The message names the request (without its query values) and page.
/// </summary>
public sealed class HttpSourceException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="message">What was wrong.</param>
    /// <param name="innerException">The underlying failure, if any.</param>
    public HttpSourceException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}

/// <summary>A response body was larger than <c>MaxResponseBytes</c>. It is never retried.</summary>
public sealed class HttpResponseTooLargeException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="endpoint">The request, without its query values.</param>
    /// <param name="limit">The limit, in bytes.</param>
    /// <param name="length">The body's length when known, in bytes.</param>
    public HttpResponseTooLargeException(string endpoint, long limit, long? length)
        : base(length is { } known
            ? $"The response from {endpoint} is {known:N0} bytes, larger than MaxResponseBytes ({limit:N0})."
            : $"The response from {endpoint} is larger than MaxResponseBytes ({limit:N0} bytes).")
    {
        Limit = limit;
    }

    /// <summary>The limit that was exceeded, in bytes.</summary>
    public long Limit { get; }
}
