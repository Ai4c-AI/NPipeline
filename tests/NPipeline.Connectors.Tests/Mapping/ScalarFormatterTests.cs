using System.Globalization;
using AwesomeAssertions;
using NPipeline.Connectors.Mapping;
using Xunit;

namespace NPipeline.Connectors.Tests.Mapping;

public sealed class ScalarFormatterTests : IDisposable
{
    private readonly CultureScope _culture = new("de-DE");

    public void Dispose() => _culture.Dispose();

    [Fact]
    public void Formats_numbers_invariantly_whatever_the_host_culture()
    {
        ScalarFormatter.Format(1234.5m).Should().Be("1234.5");
        ScalarFormatter.Format(0.1).Should().Be("0.1");
        ScalarFormatter.Format(-7L).Should().Be("-7");
    }

    [Fact]
    public void Uses_a_culture_when_given_one()
    {
        ScalarFormatter.Format(1234.5m, CultureInfo.GetCultureInfo("de-DE")).Should().Be("1234,5");
    }

    [Fact]
    public void Formats_dates_as_iso_8601_whatever_the_culture()
    {
        var provider = CultureInfo.GetCultureInfo("de-DE");

        ScalarFormatter.Format(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc), provider).Should().Be("2026-01-02T03:04:05.0000000Z");
        ScalarFormatter.Format(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.FromHours(10)), provider).Should().Be("2026-01-02T03:04:05.0000000+10:00");
        ScalarFormatter.Format(new DateOnly(2026, 1, 2), provider).Should().Be("2026-01-02");
        ScalarFormatter.Format(new TimeOnly(3, 4, 5), provider).Should().Be("03:04:05.0000000");
        ScalarFormatter.Format(new TimeSpan(1, 2, 3, 4), provider).Should().Be("1.02:03:04");
    }

    [Fact]
    public void Formats_other_scalars()
    {
        ScalarFormatter.Format(true).Should().Be("true");
        ScalarFormatter.Format(Status.Suspended).Should().Be("Suspended");
        ScalarFormatter.Format(Permissions.Read | Permissions.Write).Should().Be("Read, Write");
        ScalarFormatter.Format(new byte[] { 1, 2, 3 }).Should().Be("AQID");
        ScalarFormatter.Format(Guid.Parse("6f9619ff-8b86-d011-b42d-00cf4fc964ff")).Should().Be("6f9619ff-8b86-d011-b42d-00cf4fc964ff");
    }

    [Fact]
    public void Null_is_null()
    {
        ScalarFormatter.Format<int?>(null).Should().BeNull();
        ScalarFormatter.Format<string?>(null).Should().BeNull();
        ScalarFormatter.Format<byte[]?>(null).Should().BeNull();
        ScalarFormatter.Format<int?>(5).Should().Be("5");
    }

    [Fact]
    public void Rejects_types_that_are_not_scalars()
    {
        var format = () => ScalarFormatter.Format(new List<int>());

        format.Should().Throw<NotSupportedException>().WithMessage("*List*");
    }

    [Fact]
    public void Everything_it_writes_the_parser_reads_back_unchanged()
    {
        RoundTrip(int.MinValue);
        RoundTrip(ulong.MaxValue);
        RoundTrip(double.Epsilon);
        RoundTrip(0.1 + 0.2);
        RoundTrip(float.MaxValue);
        RoundTrip(decimal.MaxValue);
        RoundTrip(79228162514264337593543950335m / 3);
        RoundTrip('é');
        RoundTrip(new DateTime(2026, 1, 2, 3, 4, 5, 678, DateTimeKind.Utc).AddTicks(9));
        RoundTrip(new DateTimeOffset(2026, 1, 2, 3, 4, 5, 678, TimeSpan.FromHours(-9.5)));
        RoundTrip(DateOnly.MaxValue);
        RoundTrip(new TimeOnly(23, 59, 59, 999).Add(TimeSpan.FromTicks(9)));
        RoundTrip(TimeSpan.MinValue);
        RoundTrip(Guid.NewGuid());
        RoundTrip(Status.Active);
        RoundTrip(Permissions.Read | Permissions.Write);
        RoundTrip<int?>(null);
        RoundTrip<DateTime?>(new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc));

        var bytes = new byte[] { 0, 255, 16 };
        ScalarParser.Parse<byte[]>(ScalarFormatter.Format(bytes)).Should().Equal(bytes);
    }

    private static void RoundTrip<T>(T value)
    {
        var text = ScalarFormatter.Format(value);
        ScalarParser.Parse<T>(text).Should().Be(value, "'{0}' was written for it", text);
    }
}
