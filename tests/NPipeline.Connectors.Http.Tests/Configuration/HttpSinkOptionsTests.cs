using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Reliability;
using NResilience;

namespace NPipeline.Connectors.Http.Tests.Configuration;

public class HttpSinkOptionsTests
{
    private static HttpSinkOptions<int> ValidOptions() => new() { Uri = new Uri("https://api.example.com/items") };

    [Fact]
    public void Validate_WithStaticUri_DoesNotThrow()
    {
        var options = ValidOptions();
        var act = options.Validate;
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_WithUriFactory_DoesNotThrow()
    {
        var options = new HttpSinkOptions<int> { UriFactory = id => new Uri($"https://api.example.com/items/{id}") };
        var act = options.Validate;
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_WithNeitherUriNorUriFactory_ThrowsArgumentException()
    {
        var options = new HttpSinkOptions<int>();
        var act = options.Validate;

        act.Should().Throw<ArgumentException>()
            .WithMessage("*Uri*")
            .Which.ParamName.Should().Be(nameof(HttpSinkOptions<int>.Uri));
    }

    [Fact]
    public void Validate_WithRelativeUri_ThrowsArgumentException()
    {
        var options = new HttpSinkOptions<int> { Uri = new Uri("/items", UriKind.Relative) };
        var act = options.Validate;

        act.Should().Throw<ArgumentException>()
            .WithMessage("*absolute*")
            .Which.ParamName.Should().Be(nameof(HttpSinkOptions<int>.Uri));
    }

    [Fact]
    public void Validate_WithBatchSizeZero_ThrowsArgumentOutOfRangeException()
    {
        var options = ValidOptions() with { BatchSize = 0 };

        var act = options.Validate;

        act.Should().Throw<ArgumentOutOfRangeException>()
            .Which.ParamName.Should().Be(nameof(HttpSinkOptions<int>.BatchSize));
    }

    [Fact]
    public void Validate_WithInvalidResilience_ThrowsResilienceConfigurationException()
    {
        var options = ValidOptions() with
        {
#pragma warning disable NRES003 // The invalid value is the point of the test.
            Resilience = HttpConnectorResilience.Default with { Attempts = 0 },
#pragma warning restore NRES003
        };

        var act = options.Validate;

        act.Should().Throw<ResilienceConfigurationException>();
    }

    [Fact]
    public void Validate_WithIdempotencyKeyFactoryAndEmptyHeaderName_ThrowsArgumentException()
    {
        var options = ValidOptions() with
        {
            IdempotencyKeyFactory = _ => "key",
            IdempotencyHeaderName = "",
        };

        var act = options.Validate;

        act.Should().Throw<ArgumentException>()
            .Which.ParamName.Should().Be(nameof(HttpSinkOptions<int>.IdempotencyHeaderName));
    }

    [Fact]
    public void Validate_WithIdempotencyKeyFactoryAndValidHeaderName_DoesNotThrow()
    {
        var options = ValidOptions() with
        {
            IdempotencyKeyFactory = batch => $"key-{batch.Count}",
            IdempotencyHeaderName = "Idempotency-Key",
        };

        var act = options.Validate;
        act.Should().NotThrow();
    }

    [Fact]
    public void Defaults_FailRequestsAndPostSingleItems()
    {
        var options = ValidOptions();

        options.FailedRequests.Should().Be(HttpFailedRequestAction.Fail);
        options.Method.Should().Be(SinkHttpMethod.Post);
        options.BatchSize.Should().Be(1);
        options.RateLimiter.Should().BeNull();
        options.Resilience.Should().BeSameAs(HttpConnectorResilience.Default);
    }
}
