using System.Globalization;
using AwesomeAssertions;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Mapping;
using Xunit;

namespace NPipeline.Connectors.Tests.Mapping;

public sealed class ScalarParserTests : IDisposable
{
    // A comma-decimal culture: parsing must stay invariant whatever the host culture is.
    private readonly CultureScope _culture = new("de-DE");

    public void Dispose() => _culture.Dispose();

    [Theory]
    [InlineData("42", 42)]
    [InlineData(" -7 ", -7)]
    public void Parses_integers(string text, int expected) => ScalarParser.Parse<int>(text).Should().Be(expected);

    [Fact]
    public void Parses_decimals_with_the_invariant_culture() => ScalarParser.Parse<decimal>("1234.5678").Should().Be(1234.5678m);

    [Theory]
    [InlineData("1,5")]
    [InlineData("1,234")]
    [InlineData("abc")]
    [InlineData("")]
    public void Rejects_invalid_or_ambiguous_numbers(string text)
    {
        var parse = () => ScalarParser.Parse<decimal>(text);

        parse.Should().Throw<FieldConversionException>().Which.TargetType.Should().Be<decimal>();
    }

    [Fact]
    public void Uses_a_supplied_culture() => ScalarParser.Parse<decimal>("1,5", CultureInfo.GetCultureInfo("de-DE")).Should().Be(1.5m);

    [Fact]
    public void Reads_timestamps_without_an_offset_as_utc()
    {
        var value = ScalarParser.Parse<DateTime>("2026-01-02T03:04:05");

        value.Should().Be(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc));
        value.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Fact]
    public void Converts_timestamps_with_an_offset_to_utc() =>
        ScalarParser.Parse<DateTime>("2026-01-02T08:34:05+05:30").Should().Be(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc));

    [Fact]
    public void Keeps_date_time_offsets_and_assumes_utc_without_one()
    {
        ScalarParser.Parse<DateTimeOffset>("2026-01-02T03:04:05+05:30").Offset.Should().Be(TimeSpan.FromHours(5.5));
        ScalarParser.Parse<DateTimeOffset>("2026-01-02T03:04:05").Offset.Should().Be(TimeSpan.Zero);
    }

    [Theory]
    [InlineData("2026-01-02")]
    [InlineData("2026-01-02 00:00:00")]
    public void Parses_dates_and_midnight_timestamps_as_date_only(string text) =>
        ScalarParser.Parse<DateOnly>(text).Should().Be(new DateOnly(2026, 1, 2));

    [Fact]
    public void Rejects_a_timestamp_with_a_time_of_day_as_date_only()
    {
        var parse = () => ScalarParser.Parse<DateOnly>("2026-01-02 10:30:00");

        parse.Should().Throw<FieldConversionException>().WithMessage("*time of day*");
    }

    [Theory]
    [InlineData("Active", Status.Active)]
    [InlineData("suspended", Status.Suspended)]
    [InlineData("1", Status.Active)]
    public void Parses_enum_names_and_defined_numbers(string text, Status expected) => ScalarParser.Parse<Status>(text).Should().Be(expected);

    [Theory]
    [InlineData("42")]
    [InlineData("Deleted")]
    public void Rejects_undefined_enum_values(string text)
    {
        var parse = () => ScalarParser.Parse<Status>(text);

        parse.Should().Throw<FieldConversionException>();
    }

    [Fact]
    public void Accepts_flag_combinations() => ScalarParser.Parse<Permissions>("Read, Write").Should().Be(Permissions.Read | Permissions.Write);

    [Theory]
    [InlineData("true", true)]
    [InlineData("FALSE", false)]
    [InlineData("1", true)]
    [InlineData("0", false)]
    public void Parses_booleans(string text, bool expected) => ScalarParser.Parse<bool>(text).Should().Be(expected);

    [Fact]
    public void Rejects_other_boolean_words()
    {
        var parse = () => ScalarParser.Parse<bool>("yes");

        parse.Should().Throw<FieldConversionException>();
    }

    [Fact]
    public void Treats_blank_text_as_null_for_nullable_types()
    {
        ScalarParser.Parse<int?>("").Should().BeNull();
        ScalarParser.Parse<int?>("  ").Should().BeNull();
        ScalarParser.Parse<int?>("5").Should().Be(5);
    }

    [Fact]
    public void Returns_strings_unchanged() => ScalarParser.Parse<string>("  padded  ").Should().Be("  padded  ");

    [Fact]
    public void Parses_the_remaining_scalar_types()
    {
        ScalarParser.Parse<Guid>("0f8fad5b-d9cb-469f-a165-70867728950e").Should().Be(Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e"));
        ScalarParser.Parse<TimeSpan>("01:02:03").Should().Be(new TimeSpan(1, 2, 3));
        ScalarParser.Parse<TimeOnly>("13:45").Should().Be(new TimeOnly(13, 45));
        ScalarParser.Parse<char>("x").Should().Be('x');
        ScalarParser.Parse<byte[]>("AAEC/w==").Should().Equal(0, 1, 2, 255);
        ScalarParser.Parse<ulong>("18446744073709551615").Should().Be(ulong.MaxValue);
    }

    [Fact]
    public void Fails_for_types_that_are_not_scalar()
    {
        var parse = () => ScalarParser.Parse<List<int>>("1");

        parse.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void TryParse_reports_invalid_text_without_throwing()
    {
        ScalarParser.TryParse<int>("x", null, out _).Should().BeFalse();
        ScalarParser.TryParse<int>("7", null, out var value).Should().BeTrue();
        value.Should().Be(7);
    }

    [Fact]
    public void Truncates_long_raw_values_in_errors()
    {
        var parse = () => ScalarParser.Parse<int>(new string('9', 500));

        parse.Should().Throw<FieldConversionException>().Which.RawValue!.Length.Should().BeLessThan(70);
    }
}

public sealed class ScalarConverterTests
{
    [Fact]
    public void Converts_whole_doubles_to_integers() => ScalarConverter.Convert<int>(3.0).Should().Be(3);

    [Theory]
    [InlineData(3.5)]
    [InlineData(1e20)]
    [InlineData(double.NaN)]
    public void Rejects_doubles_that_are_not_whole_or_in_range(double value)
    {
        var convert = () => ScalarConverter.Convert<int>(value);

        convert.Should().Throw<FieldConversionException>();
    }

    [Fact]
    public void Converts_between_numeric_types_without_loss()
    {
        ScalarConverter.Convert<decimal>(1235.5678).Should().Be(1235.5678m);
        ScalarConverter.Convert<long>(42).Should().Be(42L);
        ScalarConverter.Convert<uint>(4_294_967_295.0).Should().Be(uint.MaxValue);
    }

    [Fact]
    public void Converts_ole_automation_dates()
    {
        var oaDate = new DateTime(2026, 1, 2, 3, 4, 5).ToOADate();

        ScalarConverter.Convert<DateTime>(oaDate).Should().Be(new DateTime(2026, 1, 2, 3, 4, 5));
        ScalarConverter.Convert<DateOnly>(new DateTime(2026, 1, 2).ToOADate()).Should().Be(new DateOnly(2026, 1, 2));
    }

    [Fact]
    public void Treats_unspecified_date_times_as_utc_for_offsets()
    {
        var value = ScalarConverter.Convert<DateTimeOffset>(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));

        value.Should().Be(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero));
    }

    [Fact]
    public void Rejects_a_date_time_with_a_time_of_day_as_date_only()
    {
        var convert = () => ScalarConverter.Convert<DateOnly>(new DateTime(2026, 1, 2, 10, 0, 0));

        convert.Should().Throw<FieldConversionException>();
    }

    [Fact]
    public void Converts_numbers_and_names_to_defined_enum_values()
    {
        ScalarConverter.Convert<Status>(2.0).Should().Be(Status.Suspended);
        ScalarConverter.Convert<Status>("active").Should().Be(Status.Active);

        var undefined = () => ScalarConverter.Convert<Status>(9L);
        undefined.Should().Throw<FieldConversionException>();
    }

    [Fact]
    public void Converts_missing_values_to_null_only_for_nullable_targets()
    {
        ScalarConverter.Convert<int?>(null).Should().BeNull();
        ScalarConverter.Convert<string>(DBNull.Value).Should().BeNull();

        var missing = () => ScalarConverter.Convert<int>(null);
        missing.Should().Throw<FieldConversionException>().WithMessage("*missing*");
    }

    [Fact]
    public void Parses_text_values_strictly()
    {
        ScalarConverter.Convert<int>("12").Should().Be(12);
        ScalarConverter.Convert<string>(new ReadOnlyMemory<char>("abc".ToCharArray())).Should().Be("abc");

        var invalid = () => ScalarConverter.Convert<Guid>("not-a-guid");
        invalid.Should().Throw<FieldConversionException>();
    }

    [Fact]
    public void Formats_values_as_invariant_text()
    {
        using var culture = new CultureScope("de-DE");

        ScalarConverter.Convert<string>(1.5).Should().Be("1.5");
        ScalarConverter.Convert<string>(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc)).Should().Be("2026-01-02T03:04:05.0000000Z");
    }

    [Fact]
    public void Converts_zero_and_one_to_booleans()
    {
        ScalarConverter.Convert<bool>(1.0).Should().BeTrue();
        ScalarConverter.Convert<bool>(0).Should().BeFalse();

        var other = () => ScalarConverter.Convert<bool>(2);
        other.Should().Throw<FieldConversionException>();
    }

    [Fact]
    public void Rejects_unrelated_types() =>
        FluentActions.Invoking(() => ScalarConverter.Convert<DateTime>(true)).Should().Throw<FieldConversionException>();
}

/// <summary>Sets the current culture for a test and restores the previous one.</summary>
public sealed class CultureScope : IDisposable
{
    private readonly CultureInfo _previous = CultureInfo.CurrentCulture;

    public CultureScope(string name) => CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(name);

    public void Dispose() => CultureInfo.CurrentCulture = _previous;
}

public enum Status
{
    Unknown,
    Active,
    Suspended,
}

[Flags]
public enum Permissions
{
    None = 0,
    Read = 1,
    Write = 2,
}
