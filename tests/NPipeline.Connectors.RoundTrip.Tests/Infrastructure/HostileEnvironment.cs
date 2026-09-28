using System.Globalization;
using System.Runtime.CompilerServices;

namespace NPipeline.Connectors.RoundTrip.Tests.Infrastructure;

/// <summary>
///     Runs every test in this assembly under a non-UTC time zone and a culture whose decimal separator is a comma.
///     CI machines default to UTC and en-US, which hides time-zone and culture bugs (for example PQ-2 and JSON-7).
/// </summary>
internal static class HostileEnvironment
{
    /// <summary>UTC+10 all year (no daylight saving), so expected offsets are stable.</summary>
    public const string TimeZoneId = "Australia/Brisbane";

    public const string CultureName = "de-DE";

    public static readonly TimeSpan ExpectedUtcOffset = TimeSpan.FromHours(10);

    [ModuleInitializer]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2255", Justification = "The environment must be set before any test runs.")]
    internal static void Apply()
    {
        // On Linux and macOS, .NET reads the TZ variable when it builds TimeZoneInfo.Local, so clearing the cache
        // applies it in-process. Windows ignores TZ; the guard test below reports that instead of failing silently.
        Environment.SetEnvironmentVariable("TZ", TimeZoneId);
        TimeZoneInfo.ClearCachedData();

        var culture = CultureInfo.GetCultureInfo(CultureName);
        CultureInfo.DefaultThreadCurrentCulture = culture;
        CultureInfo.DefaultThreadCurrentUICulture = culture;
        CultureInfo.CurrentCulture = culture;
        CultureInfo.CurrentUICulture = culture;
    }
}

public sealed class HostileEnvironmentTests
{
    [Fact]
    public void Local_time_zone_is_not_utc()
    {
        if (OperatingSystem.IsWindows())
            return; // TZ is not honoured on Windows; CI runs on Linux.

        TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).Should().Be(HostileEnvironment.ExpectedUtcOffset);
    }

    [Fact]
    public async Task Culture_uses_comma_decimal_separator_across_awaits()
    {
        await Task.Yield();

        CultureInfo.CurrentCulture.Name.Should().Be(HostileEnvironment.CultureName);
        1.5m.ToString(CultureInfo.CurrentCulture).Should().Be("1,5");
    }
}
