using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Web;

namespace NPipeline.Connectors.Http.Pagination;

/// <summary>The pagination strategies for common API styles.</summary>
public static class HttpPagination
{
    /// <summary>A single request; no further pages.</summary>
    public static IPaginationStrategy None { get; } = new DelegatePaginationStrategy(static uri => uri, static _ => null);

    /// <summary>
    ///     Numbered pages (<c>?page=2&amp;pageSize=100</c>). Stops at a page shorter than the page size, or once
    ///     <see cref="PageNumberPaginationOptions.TotalItemsJsonPath" /> says every item has been read.
    /// </summary>
    public static IPaginationStrategy PageNumber(PageNumberPaginationOptions? options = null) => new PageNumberPaginationStrategy(options ?? new());

    /// <summary>
    ///     An offset and limit (<c>?offset=200&amp;limit=100</c>). Stops at a page shorter than the limit, or once
    ///     <see cref="OffsetPaginationOptions.TotalItemsJsonPath" /> says every item has been read.
    /// </summary>
    public static IPaginationStrategy Offset(OffsetPaginationOptions? options = null) => new OffsetPaginationStrategy(options ?? new());

    /// <summary>
    ///     A cursor in the body (<c>{"meta":{"next":"abc"}}</c>) passed back as a query parameter. The cursor may be a string
    ///     or a number; paging stops when it is missing, <c>null</c> or empty.
    /// </summary>
    public static IPaginationStrategy Cursor(CursorPaginationOptions options) => new CursorPaginationStrategy(options ?? throw new ArgumentNullException(nameof(options)));

    /// <summary>The RFC 8288 <c>Link</c> header's <c>rel="next"</c> URL, as GitHub, GitLab and others send.</summary>
    public static IPaginationStrategy LinkHeader { get; } = new DelegatePaginationStrategy(static uri => uri, LinkHeaderPagination.Next);

    /// <summary>The next page's URL in the body (<c>{"next":"https://…"}</c>, absolute or relative). Paging stops when it is missing, <c>null</c> or empty.</summary>
    /// <param name="jsonPath">The dotted path to the URL, such as <c>links.next</c>.</param>
    public static IPaginationStrategy NextUrl(string jsonPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jsonPath);
        var path = HttpJsonPath.Parse(jsonPath);

        return new DelegatePaginationStrategy(static uri => uri, page =>
        {
            if (!page.TryGetValue(path, out var value) || value.ValueKind is JsonValueKind.Null)
                return null;

            if (value.ValueKind != JsonValueKind.String)
                throw new HttpSourceException($"The next-page URL at '{jsonPath}' on page {page.PageNumber} is {value.ValueKind}, not a string.");

            var text = value.GetString();

            if (string.IsNullOrEmpty(text))
                return null;

            return Uri.TryCreate(page.Uri, text, out var next) && next.IsAbsoluteUri
                ? next
                : throw new HttpSourceException($"The next-page URL at '{jsonPath}' on page {page.PageNumber} is not a valid URL: '{text}'.");
        });
    }

    /// <summary>Any other style, as a delegate.</summary>
    /// <param name="next">Returns the next page's URI given the page just read, or <c>null</c> when it was the last.</param>
    /// <param name="first">Returns the first page's URI given the base URI; by default the base URI itself.</param>
    public static IPaginationStrategy Custom(Func<HttpPageContext, Uri?> next, Func<Uri, Uri>? first = null)
    {
        ArgumentNullException.ThrowIfNull(next);
        return new DelegatePaginationStrategy(first ?? (static uri => uri), next);
    }

    internal static Uri WithQuery(Uri uri, params (string Name, string Value)[] parameters)
    {
        var builder = new UriBuilder(uri);
        var query = HttpUtility.ParseQueryString(builder.Query);

        foreach (var (name, value) in parameters)
        {
            query[name] = value;
        }

        builder.Query = query.ToString();
        return builder.Uri;
    }

    /// <summary>Reads a total-items count from the body: a number, or a string holding one.</summary>
    internal static long? ReadTotal(HttpPageContext page, string? path)
    {
        if (path is null || !page.TryGetValue(path, out var value))
            return null;

        return value.ValueKind switch
        {
            JsonValueKind.Number when value.TryGetInt64(out var number) => number,
            JsonValueKind.String when long.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var number) => number,
            JsonValueKind.Null => null,
            _ => throw new HttpSourceException($"The total at '{path}' on page {page.PageNumber} is {value.ValueKind}, not a whole number."),
        };
    }
}

/// <summary>Options for <see cref="HttpPagination.PageNumber" />.</summary>
public sealed record PageNumberPaginationOptions
{
    /// <summary>The page-number query parameter. Defaults to <c>page</c>.</summary>
    public string PageParam { get; init; } = "page";

    /// <summary>The page-size query parameter. Defaults to <c>pageSize</c>.</summary>
    public string PageSizeParam { get; init; } = "pageSize";

    /// <summary>Items per page. Defaults to 100.</summary>
    public int PageSize { get; init; } = 100;

    /// <summary>The first page's number. Defaults to 1.</summary>
    public int FirstPage { get; init; } = 1;

    /// <summary>The dotted path to the total item count in the body (<c>meta.total</c>), if the API sends one.</summary>
    public string? TotalItemsJsonPath { get; init; }
}

/// <summary>Options for <see cref="HttpPagination.Offset" />.</summary>
public sealed record OffsetPaginationOptions
{
    /// <summary>The offset query parameter. Defaults to <c>offset</c>.</summary>
    public string OffsetParam { get; init; } = "offset";

    /// <summary>The limit query parameter. Defaults to <c>limit</c>.</summary>
    public string LimitParam { get; init; } = "limit";

    /// <summary>Items per page. Defaults to 100.</summary>
    public int Limit { get; init; } = 100;

    /// <summary>The first offset. Defaults to 0.</summary>
    public long FirstOffset { get; init; }

    /// <summary>The dotted path to the total item count in the body (<c>meta.total</c>), if the API sends one.</summary>
    public string? TotalItemsJsonPath { get; init; }
}

/// <summary>Options for <see cref="HttpPagination.Cursor" />.</summary>
public sealed record CursorPaginationOptions
{
    /// <summary>The dotted path to the next cursor in the body, such as <c>meta.next_cursor</c> (a leading <c>$.</c> is allowed).</summary>
    public required string CursorJsonPath { get; init; }

    /// <summary>The query parameter that carries the cursor. Defaults to <c>cursor</c>.</summary>
    public string CursorParam { get; init; } = "cursor";

    /// <summary>A page-size query parameter to send with every request, if the API takes one.</summary>
    public string? PageSizeParam { get; init; }

    /// <summary>The page size to send with <see cref="PageSizeParam" />.</summary>
    public int? PageSize { get; init; }
}

internal sealed class DelegatePaginationStrategy(Func<Uri, Uri> first, Func<HttpPageContext, Uri?> next) : IPaginationStrategy
{
    public IPaginationCursor Start(Uri baseUri) => new Cursor(first(baseUri), next);

    private sealed class Cursor(Uri firstPage, Func<HttpPageContext, Uri?> next) : IPaginationCursor
    {
        public Uri FirstPageUri => firstPage;

        public Uri? GetNextPageUri(HttpPageContext page) => next(page);
    }
}

internal sealed class PageNumberPaginationStrategy(PageNumberPaginationOptions options) : IPaginationStrategy
{
    public IPaginationCursor Start(Uri baseUri) => new Cursor(options, baseUri);

    private sealed class Cursor(PageNumberPaginationOptions options, Uri baseUri) : IPaginationCursor
    {
        private int _page = options.FirstPage;

        public Uri FirstPageUri { get; } = Build(options, baseUri, options.FirstPage);

        public Uri? GetNextPageUri(HttpPageContext page)
        {
            if (HttpPagination.ReadTotal(page, options.TotalItemsJsonPath) is { } total && page.TotalItemCount >= total)
                return null;

            if (page.ItemCount < options.PageSize)
                return null;

            return Build(options, page.Uri, ++_page);
        }

        private static Uri Build(PageNumberPaginationOptions options, Uri uri, int page) =>
            HttpPagination.WithQuery(uri, (options.PageParam, page.ToString(CultureInfo.InvariantCulture)),
                (options.PageSizeParam, options.PageSize.ToString(CultureInfo.InvariantCulture)));
    }
}

internal sealed class OffsetPaginationStrategy(OffsetPaginationOptions options) : IPaginationStrategy
{
    public IPaginationCursor Start(Uri baseUri) => new Cursor(options, baseUri);

    private sealed class Cursor(OffsetPaginationOptions options, Uri baseUri) : IPaginationCursor
    {
        private long _offset = options.FirstOffset;

        public Uri FirstPageUri { get; } = Build(options, baseUri, options.FirstOffset);

        public Uri? GetNextPageUri(HttpPageContext page)
        {
            if (HttpPagination.ReadTotal(page, options.TotalItemsJsonPath) is { } total && options.FirstOffset + page.TotalItemCount >= total)
                return null;

            if (page.ItemCount < options.Limit)
                return null;

            _offset += page.ItemCount;
            return Build(options, page.Uri, _offset);
        }

        private static Uri Build(OffsetPaginationOptions options, Uri uri, long offset) =>
            HttpPagination.WithQuery(uri, (options.OffsetParam, offset.ToString(CultureInfo.InvariantCulture)),
                (options.LimitParam, options.Limit.ToString(CultureInfo.InvariantCulture)));
    }
}

internal sealed class CursorPaginationStrategy(CursorPaginationOptions options) : IPaginationStrategy
{
    private readonly string[] _path = HttpJsonPath.Parse(options.CursorJsonPath);

    public IPaginationCursor Start(Uri baseUri)
    {
        var first = options is { PageSizeParam: { } sizeParam, PageSize: { } size }
            ? HttpPagination.WithQuery(baseUri, (sizeParam, size.ToString(CultureInfo.InvariantCulture)))
            : baseUri;

        return new DelegatePaginationStrategy(_ => first, Next).Start(baseUri);
    }

    private Uri? Next(HttpPageContext page)
    {
        if (!page.TryGetValue(_path, out var value))
            return null;

        var cursor = value.ValueKind switch
        {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.GetRawText(),
            JsonValueKind.Null => null,
            _ => throw new HttpSourceException(
                $"The cursor at '{options.CursorJsonPath}' on page {page.PageNumber} is {value.ValueKind}, not a string or number."),
        };

        return string.IsNullOrEmpty(cursor) ? null : HttpPagination.WithQuery(page.Uri, (options.CursorParam, cursor));
    }
}

internal static partial class LinkHeaderPagination
{
    public static Uri? Next(HttpPageContext page)
    {
        if (!page.Headers.TryGetValues("Link", out var values))
            return null;

        foreach (var header in values)
        {
            foreach (var part in header.Split(','))
            {
                var match = LinkRel().Match(part.Trim());

                if (match.Success && match.Groups[2].Value.Split(' ').Contains("next", StringComparer.OrdinalIgnoreCase)
                                  && Uri.TryCreate(page.Uri, match.Groups[1].Value, out var next) && next.IsAbsoluteUri)
                    return next;
            }
        }

        return null;
    }

    [GeneratedRegex(@"<([^>]+)>\s*;\s*rel=""?([^"";]+)""?", RegexOptions.IgnoreCase)]
    private static partial Regex LinkRel();
}
