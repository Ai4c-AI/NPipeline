using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Pagination;
using NPipeline.Connectors.Http.Reliability;
using NResilience;

namespace NPipeline.Connectors.Http.Tests.Configuration;

public class HttpSourceOptionsTests
{
    private static HttpSourceOptions<int> ValidOptions() => new() { BaseUri = new Uri("https://api.example.com/items") };

    [Fact]
    public void Validate_WithAbsoluteUri_DoesNotThrow()
    {
        var options = ValidOptions();
        var act = options.Validate;

        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_WithRelativeUri_ThrowsArgumentException()
    {
        var options = new HttpSourceOptions<int> { BaseUri = new Uri("/relative/path", UriKind.Relative) };
        var act = options.Validate;

        act.Should().Throw<ArgumentException>()
            .WithMessage("*BaseUri*")
            .Which.ParamName.Should().Be(nameof(HttpSourceOptions<int>.BaseUri));
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
    public void Validate_WithMaxPagesZero_ThrowsArgumentOutOfRangeException()
    {
        var options = ValidOptions() with { MaxPages = 0 };

        var act = options.Validate;

        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithMessage("*MaxPages*")
            .Which.ParamName.Should().Be(nameof(HttpSourceOptions<int>.MaxPages));
    }

    [Fact]
    public void Validate_WithMaxResponseBytesZero_ThrowsArgumentOutOfRangeException()
    {
        var options = ValidOptions() with { MaxResponseBytes = 0 };

        var act = options.Validate;

        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithMessage("*MaxResponseBytes*")
            .Which.ParamName.Should().Be(nameof(HttpSourceOptions<int>.MaxResponseBytes));
    }

    [Fact]
    public void Validate_WithNegativeRawExcerptLength_ThrowsArgumentOutOfRangeException()
    {
        var options = ValidOptions() with { RawExcerptLength = -1 };

        var act = options.Validate;

        act.Should().Throw<ArgumentOutOfRangeException>()
            .Which.ParamName.Should().Be(nameof(HttpSourceOptions<int>.RawExcerptLength));
    }

    [Fact]
    public void Validate_WithPositiveMaxPages_DoesNotThrow()
    {
        var options = ValidOptions() with { MaxPages = 10 };

        var act = options.Validate;
        act.Should().NotThrow();
    }

    [Fact]
    public void Defaults_SendASingleGetWithTheDefaultResilience()
    {
        var options = ValidOptions();

        options.RequestMethod.Should().Be(HttpMethod.Get);
        options.Pagination.Should().BeSameAs(HttpPagination.None);
        options.RateLimiter.Should().BeNull();
        options.Resilience.Should().BeSameAs(HttpConnectorResilience.Default);
        options.RawExcerptLength.Should().Be(HttpSourceOptions<int>.DefaultRawExcerptLength);
    }
}
