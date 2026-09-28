using System.Text;

namespace NPipeline.Connectors.Http.Reliability;

/// <summary>
///     URIs as they appear outside the process. Query strings carry page numbers, cursors and sometimes API keys, so
///     metric labels drop the query (bounded cardinality) and traces, logs and errors redact its values.
/// </summary>
internal static class HttpRedaction
{
    public const string Redacted = "REDACTED";

    /// <summary>The URI without its query or fragment, for metric labels: <c>https://api.test/items</c>.</summary>
    public static string Endpoint(Uri uri) =>
        uri.IsAbsoluteUri ? uri.GetLeftPart(UriPartial.Path) : uri.OriginalString.Split('?', '#')[0];

    /// <summary>The URI with each query value replaced, for traces, logs and errors: <c>https://api.test/items?page=REDACTED</c>.</summary>
    public static string Full(Uri uri)
    {
        var endpoint = Endpoint(uri);
        var query = uri.IsAbsoluteUri ? uri.Query : string.Empty;

        if (query.Length <= 1)
            return endpoint;

        var builder = new StringBuilder(endpoint).Append('?');
        var first = true;

        foreach (var pair in query[1..].Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var separator = pair.IndexOf('=', StringComparison.Ordinal);
            _ = builder.Append(first ? string.Empty : "&").Append(separator < 0 ? pair : pair[..separator]).Append('=').Append(Redacted);
            first = false;
        }

        return builder.ToString();
    }
}
