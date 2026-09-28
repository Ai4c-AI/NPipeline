using System.Diagnostics;
using System.Net;
using System.Threading.RateLimiting;
using NPipeline.Connectors.Http.Metrics;
using NResilience;

namespace NPipeline.Connectors.Http.Reliability;

/// <summary>
///     Sends requests through an <see cref="HttpResilienceHandler" /> placed in front of any <see cref="HttpClient" />.
/// </summary>
/// <remarks>
///     The nodes accept a client from <see cref="IHttpClientFactory" /> or from the caller, so the resilience handler
///     cannot sit inside the client's own pipeline. Instead the client becomes the handler's transport: every attempt
///     is a separate <see cref="HttpClient.SendAsync(HttpRequestMessage, HttpCompletionOption, CancellationToken)" />,
///     so the client's base address, default headers, and handlers still apply to each one. This is the only layer
///     that retries; do not also add a retrying handler to the client.
/// </remarks>
internal sealed class ResilientHttpSender : IDisposable
{
    private static readonly ActivitySource ActivitySource = new("NPipeline.Connectors.Http");

    private readonly HttpMessageInvoker _invoker;

    public ResilientHttpSender(
        HttpClient client,
        Resilience policy,
        bool bufferResponses,
        IHttpConnectorMetrics metrics,
        Action<CallEvent> listener,
        RateLimiter? rateLimiter = null,
        long? maxResponseBytes = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(policy);

        // HttpContent wraps a failure while reading the body in an HttpRequestException, which the HTTP classifier
        // retries. A body over MaxResponseBytes will be just as large next time, so it is never retried.
        var classifier = policy.Classifier;

        var limited = policy with
        {
            Classifier = classifier
                .On<HttpResponseTooLargeException>(Verdict.Permanent)
                .On<HttpRequestException>(e => IsTooLarge(e) ? Verdict.Permanent : classifier.ClassifyException(e)),
        };

        var options = new HttpResilienceOptions { BufferResponses = bufferResponses };
        var transport = new ClientTransport(client, metrics, rateLimiter, maxResponseBytes);
        var handler = new HttpResilienceHandler(transport, limited.WithListener(listener), options);
        _invoker = new HttpMessageInvoker(handler, true);
    }

    public void Dispose()
    {
        _invoker.Dispose();
    }

    /// <summary>
    ///     Sends the request, retrying as the policy allows, and returns the final response. A response that is still a
    ///     failure once retries are spent is returned rather than thrown, so the caller judges it.
    /// </summary>
    /// <exception cref="HttpResponseTooLargeException">The body is larger than the limit.</exception>
    public async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        try
        {
            return await _invoker.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (TooLarge(ex) is { } tooLarge && tooLarge != ex)
        {
            throw tooLarge;
        }
    }

    private static bool IsTooLarge(Exception exception) => TooLarge(exception) is not null;

    private static HttpResponseTooLargeException? TooLarge(Exception? exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is HttpResponseTooLargeException tooLarge)
                return tooLarge;
        }

        return null;
    }

    /// <summary>
    ///     Forwards each attempt to the caller's client, which the sender does not own. Each attempt takes a rate-limiter
    ///     lease, is traced and measured on its own, and has its body length checked before anything buffers it.
    /// </summary>
    private sealed class ClientTransport(HttpClient client, IHttpConnectorMetrics metrics, RateLimiter? rateLimiter, long? maxResponseBytes)
        : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var uri = request.RequestUri!;
            var endpoint = HttpRedaction.Endpoint(uri);
            var method = request.Method.Method;

            // Retries take a lease too, so a burst of 429 or 503 retries cannot exceed the limit that caused it.
            using var lease = await AcquireAsync(endpoint, cancellationToken).ConfigureAwait(false);

            using var activity = ActivitySource.StartActivity(method, ActivityKind.Client);

            if (activity is not null)
            {
                _ = activity.SetTag("http.request.method", method);
                _ = activity.SetTag("url.full", HttpRedaction.Full(uri));
                _ = activity.SetTag("server.address", uri.Host);
                _ = activity.SetTag("server.port", uri.Port);
            }

            metrics.RecordRequest(endpoint, method);
            var started = Stopwatch.GetTimestamp();
            HttpResponseMessage response;

            try
            {
                response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _ = activity?.SetTag("error.type", ex.GetType().FullName);
                _ = activity?.SetStatus(ActivityStatusCode.Error);
                throw;
            }

            metrics.RecordResponse(endpoint, method, (int)response.StatusCode, Stopwatch.GetElapsedTime(started));
            _ = activity?.SetTag("http.response.status_code", (int)response.StatusCode);

            if (!response.IsSuccessStatusCode)
            {
                _ = activity?.SetTag("error.type", ((int)response.StatusCode).ToString(System.Globalization.CultureInfo.InvariantCulture));
                _ = activity?.SetStatus(ActivityStatusCode.Error);
            }

            if (maxResponseBytes is { } limit && response.IsSuccessStatusCode)
            {
                if (response.Content.Headers.ContentLength is { } length && length > limit)
                {
                    response.Dispose();
                    throw new HttpResponseTooLargeException(endpoint, limit, length);
                }

                response.Content = new LimitedContent(response.Content, limit, endpoint);
            }

            return response;
        }

        private async ValueTask<RateLimitLease?> AcquireAsync(string endpoint, CancellationToken cancellationToken)
        {
            if (rateLimiter is null)
                return null;

            var started = Stopwatch.GetTimestamp();
            var lease = await rateLimiter.AcquireAsync(1, cancellationToken).ConfigureAwait(false);
            metrics.RecordRateLimitWait(endpoint, Stopwatch.GetElapsedTime(started));

            if (lease.IsAcquired)
                return lease;

            lease.Dispose();
            throw new InvalidOperationException($"The rate limiter rejected the request to {endpoint}: its queue is full.");
        }
    }

    /// <summary>Response content that fails as soon as more than the limit has been read, whoever reads it.</summary>
    private sealed class LimitedContent : HttpContent
    {
        private readonly string _endpoint;
        private readonly HttpContent _inner;
        private readonly long _limit;

        public LimitedContent(HttpContent inner, long limit, string endpoint)
        {
            _inner = inner;
            _limit = limit;
            _endpoint = endpoint;

            foreach (var header in inner.Headers)
            {
                _ = Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            var source = await CreateContentReadStreamAsync().ConfigureAwait(false);

            await using (source.ConfigureAwait(false))
            {
                await source.CopyToAsync(stream).ConfigureAwait(false);
            }
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken)
        {
            var source = await CreateContentReadStreamAsync(cancellationToken).ConfigureAwait(false);

            await using (source.ConfigureAwait(false))
            {
                await source.CopyToAsync(stream, cancellationToken).ConfigureAwait(false);
            }
        }

        protected override async Task<Stream> CreateContentReadStreamAsync() =>
            new LimitedStream(await _inner.ReadAsStreamAsync().ConfigureAwait(false), _limit, _endpoint);

        protected override async Task<Stream> CreateContentReadStreamAsync(CancellationToken cancellationToken) =>
            new LimitedStream(await _inner.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false), _limit, _endpoint);

        protected override bool TryComputeLength(out long length)
        {
            length = _inner.Headers.ContentLength ?? 0;
            return _inner.Headers.ContentLength.HasValue;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _inner.Dispose();

            base.Dispose(disposing);
        }
    }

    private sealed class LimitedStream(Stream inner, long limit, string endpoint) : Stream
    {
        private long _read;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => _read;
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) => Count(inner.Read(buffer, offset, count));

        public override int Read(Span<byte> buffer) => Count(inner.Read(buffer));

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            Count(await inner.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false));

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            Count(await inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false));

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();

            base.Dispose(disposing);
        }

        private int Count(int read)
        {
            _read += read;
            return _read > limit ? throw new HttpResponseTooLargeException(endpoint, limit, null) : read;
        }
    }
}
