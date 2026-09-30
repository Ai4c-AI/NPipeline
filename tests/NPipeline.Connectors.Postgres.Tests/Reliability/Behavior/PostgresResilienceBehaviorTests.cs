using AwesomeAssertions;
using Npgsql;
using NPipeline.Connectors.Postgres.Reliability;
using NResilience;

namespace NPipeline.Connectors.Postgres.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the PostgreSQL connector's NResilience policy (SQL1 and X1 in <c>plans/resilience-improvements.md</c>).
///     They assert what reached the database, not what is configured.
/// </summary>
public sealed class PostgresResilienceBehaviorTests
{
    [Fact]
    public void DefaultPreset_PreservesTheAttemptCountsAndDelaysOfTheSettingsItReplaces()
    {
        var preset = PostgresConnectorResilience.Default;

        // The COPY writer made MaxRetryAttempts + 1 = four calls with RetryDelay = 1 s doubled per retry. (The source
        // made only MaxRetryAttempts = three, an off-by-one against the setting's documented meaning; it now makes four.)
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromSeconds(1));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(30));
        preset.Backoff.ThrottledBase.Should().Be(TimeSpan.FromSeconds(5));

        // A COPY of many rows must not be cut off at NResilience's 10 s attempt timeout or 30 s deadline.
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse("the preset reproduces a fixed attempt count");
        preset.Validate();
    }

    [Theory]
    [InlineData("53300", VerdictKind.Throttled)]
    [InlineData("40001", VerdictKind.Transient)]
    [InlineData("40P01", VerdictKind.Transient)]
    [InlineData("08006", VerdictKind.Transient)]
    [InlineData("57P01", VerdictKind.Transient)]
    [InlineData("23505", VerdictKind.Permanent)]
    [InlineData("42P01", VerdictKind.Permanent)]
    public void Classifier_JudgesServerErrorsBySqlState(string sqlState, VerdictKind expected)
    {
        var exception = new PostgresException("injected", "ERROR", "ERROR", sqlState);

        PostgresConnectorResilience.Classifier.ClassifyException(exception).Kind.Should().Be(expected);
    }

    [Fact]
    public void Classifier_FollowsNpgsqlForClientErrors_AndTreatsForeignCancellationAsPermanent()
    {
        var classifier = PostgresConnectorResilience.Classifier;

        classifier.ClassifyException(new NpgsqlException("broken", new IOException())).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new NpgsqlException("protocol violation")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new TimeoutException()).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new OperationCanceledException()).Kind.Should().Be(VerdictKind.Permanent);
    }
}
