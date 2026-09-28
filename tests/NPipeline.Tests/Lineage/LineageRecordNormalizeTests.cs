using AwesomeAssertions;
using NPipeline.Lineage;

namespace NPipeline.Tests.Lineage;

public sealed class LineageRecordNormalizeTests
{
    [Fact]
    public void Normalize_WithTimestampAndNoContributors_ReturnsSameInstance()
    {
        // Arrange
        var record = CreateRecord() with { TimestampUtc = DateTimeOffset.UtcNow };

        // Act
        var normalized = record.Normalize();

        // Assert
        normalized.Should().BeSameAs(record);
    }

    [Fact]
    public void Normalize_WithoutTimestamp_SetsOne()
    {
        // Act
        var normalized = CreateRecord().Normalize();

        // Assert
        normalized.TimestampUtc.Should().NotBe(default);
    }

    [Fact]
    public void Normalize_WithContributors_CopiesSortedAndDistinct()
    {
        // Arrange
        List<int> indices = [3, 1, 3];
        var record = CreateRecord() with { TimestampUtc = DateTimeOffset.UtcNow, ContributorInputIndices = indices };

        // Act
        var normalized = record.Normalize();
        indices.Add(0);

        // Assert
        normalized.Should().NotBeSameAs(record);
        normalized.ContributorInputIndices.Should().Equal(1, 3);
    }

    private static LineageRecord CreateRecord() =>
        new(Guid.NewGuid(), "node", Guid.NewGuid(), LineageOutcomeReason.Emitted, false, ["node"]);
}
