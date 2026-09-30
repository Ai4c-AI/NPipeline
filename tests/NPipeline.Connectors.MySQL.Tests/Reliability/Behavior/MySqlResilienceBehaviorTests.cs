using AwesomeAssertions;
using MySqlConnector;
using NPipeline.Connectors.MySql.Reliability;
using NResilience;

namespace NPipeline.Connectors.MySql.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the MySQL connector's NResilience policy (SQL1 and X1 in <c>plans/resilience-improvements.md</c>).
///     They assert what reached the database, not what is configured.
/// </summary>
public sealed class MySqlResilienceBehaviorTests
{
    [Fact]
    public void DefaultPreset_PreservesTheAttemptCountsAndDelaysOfTheSettingsItReplaces()
    {
        var preset = MySqlConnectorResilience.Default;

        // MaxRetryAttempts = 3 made MaxRetryAttempts + 1 = four calls; RetryDelay = 2 s doubled per retry, capped at 30 s.
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromSeconds(2));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(30));
        preset.Backoff.ThrottledBase.Should().Be(TimeSpan.FromSeconds(5));

        // A bulk load of many rows must not be cut off at NResilience's 10 s attempt timeout or 30 s deadline.
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse("the preset reproduces a fixed attempt count");
        preset.Validate();
    }

    [Theory]
    [InlineData(1040, VerdictKind.Throttled)]
    [InlineData(1203, VerdictKind.Throttled)]
    [InlineData(1205, VerdictKind.Transient)]
    [InlineData(1213, VerdictKind.Transient)]
    [InlineData(2006, VerdictKind.Transient)]
    [InlineData(2013, VerdictKind.Transient)]
    [InlineData(1062, VerdictKind.Permanent)]
    [InlineData(1146, VerdictKind.Permanent)]
    public void Classifier_JudgesServerErrorsByNumber(int number, VerdictKind expected)
    {
        MySqlConnectorResilience.Classifier.ClassifyException(MySqlExceptions.WithNumber(number)).Kind.Should().Be(expected);
    }

    [Fact]
    public void Classifier_JudgesOtherExceptionsLikeTheDetector_ExceptCancellation()
    {
        var classifier = MySqlConnectorResilience.Classifier;

        classifier.ClassifyException(new TimeoutException()).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new InvalidOperationException("Connection must be Open.")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new InvalidOperationException("Bad mapping.")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new ObjectDisposedException("MySqlConnection")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new OperationCanceledException()).Kind.Should().Be(VerdictKind.Permanent);
    }
}
