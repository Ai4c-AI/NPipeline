using AwesomeAssertions;
using Microsoft.Data.SqlClient;
using NPipeline.Connectors.SqlServer.Reliability;
using NResilience;

namespace NPipeline.Connectors.SqlServer.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the SQL Server connector's NResilience policy (SQL1 and X1 in <c>plans/resilience-improvements.md</c>).
///     The retries themselves are tested on the shared SQL sink.
/// </summary>
public sealed class SqlServerResilienceBehaviorTests
{
    [Fact]
    public void DefaultPreset_PreservesTheAttemptCountsAndDelaysOfTheSettingsItReplaces()
    {
        var preset = SqlServerConnectorResilience.Default;

        // MaxRetryAttempts = 3 made MaxRetryAttempts + 1 = four calls; RetryDelay = 1 s doubled per retry, capped at 30 s.
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromSeconds(1));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(30));
        preset.Backoff.ThrottledBase.Should().Be(TimeSpan.FromSeconds(10));

        // A bulk copy of many rows must not be cut off at NResilience's 10 s attempt timeout or 30 s deadline; the
        // driver's CommandTimeout and BulkCopyTimeout bound each attempt instead.
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse("the preset reproduces a fixed attempt count");
        preset.Validate();
    }

    [Theory]
    [InlineData(40501)]
    [InlineData(10928)]
    [InlineData(10929)]
    [InlineData(49918)]
    public void Classifier_TreatsAzureThrottlingAsThrottled(int number)
    {
        SqlServerConnectorResilience.Classifier.ClassifyException(SqlExceptions.WithNumber(number)).Kind
            .Should().Be(VerdictKind.Throttled);
    }

    [Theory]
    [InlineData(-2)]
    [InlineData(1205)]
    [InlineData(40613)]
    public void Classifier_TreatsTheDetectorsTransientErrorsAsTransient(int number)
    {
        SqlServerConnectorResilience.Classifier.ClassifyException(SqlExceptions.WithNumber(number)).Kind
            .Should().Be(VerdictKind.Transient);
    }

    [Theory]
    [InlineData(2627)]
    [InlineData(547)]
    [InlineData(208)]
    public void Classifier_TreatsOtherSqlErrorsAsPermanent(int number)
    {
        SqlServerConnectorResilience.Classifier.ClassifyException(SqlExceptions.WithNumber(number)).Kind
            .Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public void Classifier_JudgesOtherExceptionsLikeTheDetector_ExceptCancellation()
    {
        var classifier = SqlServerConnectorResilience.Classifier;

        classifier.ClassifyException(new TimeoutException()).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new InvalidOperationException("The connection is broken.")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new InvalidOperationException("Bad mapping.")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new ObjectDisposedException("SqlConnection")).Kind.Should().Be(VerdictKind.Permanent);

        // An OperationCanceledException the pipeline did not ask for is a failure, not something to retry.
        classifier.ClassifyException(new OperationCanceledException()).Kind.Should().Be(VerdictKind.Permanent);
    }
}
