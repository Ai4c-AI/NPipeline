using System.Data.Common;
using System.Net;
using AwesomeAssertions;
using NPipeline.Connectors.Snowflake.Exceptions;
using NPipeline.Connectors.Snowflake.Reliability;
using NResilience;
using Snowflake.Data.Client;

namespace NPipeline.Connectors.Snowflake.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the Snowflake connector's NResilience policy (SQL1 and X1 in <c>plans/resilience-improvements.md</c>).
///     They assert what reached the database, not what is configured. There is no Snowflake emulator, so the writers run
///     against a scripted connection.
/// </summary>
public sealed class SnowflakeResilienceBehaviorTests
{
    private const int NetworkError = 200002;
    private const int ObjectDoesNotExist = 2003;
    private const int DriverRequestTimeout = 270007;
    private const int DriverPutIoError = 270058;
    private const int SessionGone = 390111;

    [Fact]
    public void DefaultPreset_PreservesTheAttemptCountsAndDelaysOfTheSettingsItReplaces()
    {
        var preset = SnowflakeConnectorResilience.Default;

        // MaxRetryAttempts = 3 made MaxRetryAttempts + 1 = four calls; RetryDelay = 2 s doubled per retry, capped at 60 s.
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromSeconds(2));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(60));
        preset.Backoff.ThrottledBase.Should().Be(TimeSpan.FromSeconds(10));

        // A staged COPY INTO can run for minutes; NResilience's 10 s attempt timeout and 30 s deadline would cut it off.
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse("the preset reproduces a fixed attempt count");
        preset.Validate();
    }

    [Fact]
    public void Classifier_RetriesStatementLevelErrorsTheServerReturns()
    {
        var classifier = SnowflakeConnectorResilience.Classifier;

        classifier.ClassifyException(Server(390144, "Service unavailable")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(Server(625, "Statement reached its statement or warehouse timeout")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(Server(604, "SQL execution internal error")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(Server(1234, "Request throttled")).Kind.Should().Be(VerdictKind.Throttled);

        classifier.ClassifyException(Server(ObjectDoesNotExist, "Object does not exist")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(Server(1003, "SQL compilation error")).Kind.Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public void Classifier_TreatsFailuresTheDriverAlreadyRetriedAsPermanent()
    {
        // Snowflake.Data retries every HTTP request itself (transport errors, timeouts, 5xx, 403, 408, 429). What it
        // reports after giving up must not be retried again by rerunning the statement.
        var classifier = SnowflakeConnectorResilience.Classifier;

        classifier.ClassifyException(new HttpRequestException("busy", null, HttpStatusCode.TooManyRequests)).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new HttpRequestException("unavailable", null, HttpStatusCode.ServiceUnavailable)).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new HttpRequestException("reset")).Kind.Should().Be(VerdictKind.Permanent);

        var requestTimeout = Server(DriverRequestTimeout, "Request reach its timeout");
        classifier.ClassifyException(requestTimeout).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new OperationCanceledException(requestTimeout.Message, requestTimeout)).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(Server(DriverPutIoError, "IO error on PUT, network unreachable")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(Server(SessionGone, "Session no longer exists")).Kind.Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public void Classifier_JudgesOtherExceptions()
    {
        var classifier = SnowflakeConnectorResilience.Classifier;

        classifier.ClassifyException(new TimeoutException()).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new InvalidOperationException("Bad mapping.")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new ObjectDisposedException("SnowflakeDbConnection")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new OperationCanceledException()).Kind.Should().Be(VerdictKind.Permanent);
    }

    private static SnowflakeDbException Server(int vendorCode, string message) => new("XX000", vendorCode, message, "query-id");
}
