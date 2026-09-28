# NPipeline.Connectors.Http

REST API source and sink nodes for NPipeline. The source follows pagination and reads each page's body once; the sink
posts items singly or in batches. Both retry through NResilience, take a rate-limiter lease for every attempt, and keep
query values (which can hold API keys) out of metrics, traces, logs and errors.

## Features

- **Pagination**: page numbers, offsets, cursors (string or number), RFC 8288 `Link` headers, next-page URLs in the
  body, or a delegate. Each run pages independently, so one options instance can be shared.
- **Strict reading**: a wrong `ItemsJsonPath`, a non-array response or a non-JSON body fails with what the response
  holds, instead of returning no items. Items that do not convert are row errors: fail, skip or dead-letter.
- **Bounded memory**: `MaxResponseBytes` is enforced while the body is read, before anything buffers it.
- **Writing**: POST, PUT or PATCH; batches, wrapper keys, per-item endpoints, idempotency keys, and failed requests
  failed, skipped or sent to the dead-letter sink with their items.
- **Resilience**: retries with exponential backoff, `Retry-After`, per-host circuit breakers, and no retries of POST or
  PATCH without an idempotency key.
- **Rate limiting**: any `System.Threading.RateLimiting.RateLimiter`, applied to retries too.
- **Telemetry**: OpenTelemetry client spans with redacted `url.full`; metrics labelled by endpoint without the query.
- **Source generation**: pass a `JsonTypeInfo<T>` for trimming and Native AOT.

## Installation

```bash
dotnet add package NPipeline.Connectors.Http
```

## Quick start

```csharp
using NPipeline.Connectors.Http;
using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.Pagination;

public sealed record GithubRelease(string TagName, string Name, DateTime PublishedAt);
public sealed record SlackMessage(string Text);

using var httpClient = new HttpClient();

var releases = HttpConnector.Source<GithubRelease>(new Uri("https://api.github.com/repos/dotnet/runtime/releases"), httpClient, o => o with
{
    Headers = new Dictionary<string, string> { ["User-Agent"] = "MyApp/1.0" },
    Auth = new BearerTokenAuthProvider(Environment.GetEnvironmentVariable("GITHUB_TOKEN")!),
    Pagination = HttpPagination.LinkHeader,
    MaxPages = 5,
});

var slack = HttpConnector.Sink<SlackMessage>(new Uri("https://hooks.slack.com/services/YOUR/WEBHOOK/URL"), httpClient);
```

A wrapped response with a cursor:

```csharp
var orders = HttpConnector.Source<Order>(new Uri("https://api.example.com/orders"), httpClient, o => o with
{
    ItemsJsonPath = "data",
    Pagination = HttpPagination.Cursor(new CursorPaginationOptions { CursorJsonPath = "meta.next_cursor" }),
    RowErrorHandler = _ => RowErrorAction.Skip,
});
```

## Documentation

See the [HTTP connector documentation](https://docs.npipeline.net/connectors/http) for every option, and the
[`samples/Sample_HttpConnector`](../../samples/Sample_HttpConnector) project for a GitHub-to-Slack pipeline:

```bash
GITHUB_TOKEN=ghp_... SLACK_WEBHOOK=https://hooks.slack.com/... \
dotnet run --project samples/Sample_HttpConnector
```

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.
