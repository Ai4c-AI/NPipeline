using System.Net;

namespace NPipeline.Connectors.Http.Models;

/// <summary>
///     A request an HTTP sink could not complete once retries were spent, sent to the pipeline's dead-letter sink when
///     <see cref="Configuration.HttpSinkOptions{T}.FailedRequests" /> is <see cref="Configuration.HttpFailedRequestAction.DeadLetter" />.
/// </summary>
/// <param name="Endpoint">The request URI, with query values redacted.</param>
/// <param name="Method">The request method.</param>
/// <param name="StatusCode">The final response's status.</param>
/// <param name="ResponseExcerpt">The start of the response body (512 characters at most), or <c>null</c> when it was empty.</param>
/// <param name="Items">The items the request carried, in order, so they can be replayed.</param>
/// <typeparam name="T">The item type.</typeparam>
public sealed record HttpRequestFailure<T>(string Endpoint, string Method, HttpStatusCode StatusCode, string? ResponseExcerpt, IReadOnlyList<T> Items);
