using NPipeline.Connectors.RoundTrip.Tests.Harnesses;
using NPipeline.Connectors.RoundTrip.Tests.Models;

namespace NPipeline.Connectors.RoundTrip.Tests;

/// <summary>
///     Round trips shared by every connector: write through the sink, read back through the source, and expect the same
///     items in the same order. Each connector's test class calls the scenarios its format supports.
/// </summary>
public static class RoundTripScenarios
{
    public static Task Scalars(ConnectorHarness harness) =>
        AssertRoundTrip(harness, [.. Enumerable.Range(1, 20).Select(ScalarRecord.Create)]);

    public static Task SmallAndUnsignedIntegers(ConnectorHarness harness) =>
        AssertRoundTrip(harness,
        [
            new UnsignedRecord { Id = 1, U8 = byte.MaxValue, S8 = sbyte.MinValue, S16 = short.MinValue, U16 = ushort.MaxValue, U32 = uint.MaxValue, U64 = 9_007_199_254_740_000UL },
            new UnsignedRecord { Id = 2, U8 = 1, S8 = 1, S16 = 1, U16 = 1, U32 = 1, U64 = 1 },
        ]);

    public static Task Nullables(ConnectorHarness harness) =>
        AssertRoundTrip(harness,
        [
            new NullableRecord { Id = 1 },
            new NullableRecord { Id = 2, Count = 7, Price = 12.3456m, Score = 0.5, Flag = true, When = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc) },
        ]);

    public static Task Enums(ConnectorHarness harness) =>
        AssertRoundTrip(harness,
        [
            new EnumRecord { Id = 1, Status = Status.Active },
            new EnumRecord { Id = 2, Status = Status.Suspended, Optional = Status.Active },
        ]);

    /// <summary>Formats may normalise the offset (Parquet stores UTC), but the instant must survive.</summary>
    public static async Task DateTimeOffsets(ConnectorHarness harness)
    {
        DateTimeOffsetRecord[] items =
        [
            new() { Id = 1, At = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.FromHours(5.5)) },
            new() { Id = 2, At = new DateTimeOffset(2026, 7, 8, 9, 10, 11, TimeSpan.FromHours(-8)) },
            new() { Id = 3, At = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero) },
        ];

        var actual = await harness.RoundTripAsync(items);

        actual.Select(i => (i.Id, i.At.UtcDateTime)).Should().Equal(items.Select(i => (i.Id, i.At.UtcDateTime)));
    }

    public static Task DateOnlys(ConnectorHarness harness) =>
        AssertRoundTrip(harness,
        [
            new DateOnlyRecord { Id = 1, Day = new DateOnly(2026, 1, 2) },
            new DateOnlyRecord { Id = 2, Day = new DateOnly(1999, 12, 31) },
        ]);

    public static Task Text(ConnectorHarness harness) =>
        AssertRoundTrip(harness, [.. TextRecord.TrickyValues.Select((text, i) => new TextRecord { Id = i, Text = text })]);

    public static Task ControlCharacters(ConnectorHarness harness) =>
        AssertRoundTrip(harness,
        [
            new ControlCharacterRecord { Id = 1, Text = "bell\u0007" },
            new ControlCharacterRecord { Id = 2, Text = "unit\u001Fseparator" },
        ]);

    public static Task Binary(ConnectorHarness harness) =>
        AssertRoundTrip(harness,
        [
            new BinaryRecord { Id = 1, Data = [0, 1, 2, 255] },
            new BinaryRecord { Id = 2, Data = null },
        ]);

    public static Task Lists(ConnectorHarness harness) =>
        AssertRoundTrip(harness,
        [
            new ListRecord { Id = 1, Values = [1, 2, 3] },
            new ListRecord { Id = 2, Values = [42] },
        ]);

    public static Task Nested(ConnectorHarness harness) =>
        AssertRoundTrip(harness,
        [
            new NestedRecord { Id = 1, Address = new Address { Street = "1 Main St", Postcode = "4000" } },
            new NestedRecord { Id = 2, Address = null },
        ]);

    public static Task PositionalRecords(ConnectorHarness harness) =>
        AssertRoundTrip(harness, [new PositionalRecord(1, "Ada"), new PositionalRecord(2, "Grace")]);

    /// <summary>Enough rows to cross buffer and row-group boundaries; order must be preserved.</summary>
    public static Task Volume(ConnectorHarness harness) =>
        AssertRoundTrip(harness, [.. Enumerable.Range(0, 25_000).Select(ScalarRecord.Create)]);

    public static async Task Empty(ConnectorHarness harness)
    {
        var actual = await harness.RoundTripAsync(Array.Empty<ScalarRecord>());

        actual.Should().BeEmpty();
    }

    private static async Task AssertRoundTrip<T>(ConnectorHarness harness, IReadOnlyList<T> items)
        where T : notnull
    {
        var actual = await harness.RoundTripAsync(items);

        actual.Should().BeEquivalentTo(items, options => options.WithStrictOrdering());
    }
}
