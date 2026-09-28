using System.Text;
using NPipeline.Connectors.Csv;
using NPipeline.Connectors.RoundTrip.Tests.Harnesses;
using NPipeline.Connectors.RoundTrip.Tests.Infrastructure;
using NPipeline.Connectors.RoundTrip.Tests.Models;

namespace NPipeline.Connectors.RoundTrip.Tests;

public sealed class CsvRoundTripTests
{
    private readonly CsvHarness _harness = new();

    [Fact]
    public Task Scalars() => RoundTripScenarios.Scalars(_harness);

    [Fact]
    public Task SmallAndUnsignedIntegers() => RoundTripScenarios.SmallAndUnsignedIntegers(_harness);

    [Fact]
    public Task Nullables() => RoundTripScenarios.Nullables(_harness);

    [Fact]
    public Task Enums() => RoundTripScenarios.Enums(_harness);

    [Fact]
    public Task DateTimeOffsets() => RoundTripScenarios.DateTimeOffsets(_harness);

    [Fact]
    public Task DateOnlys() => RoundTripScenarios.DateOnlys(_harness);

    [Fact]
    public Task Text() => RoundTripScenarios.Text(_harness);

    [Fact]
    public Task ControlCharacters() => RoundTripScenarios.ControlCharacters(_harness);

    // No Binary round trip: CSV cannot tell a null byte[] from an empty one.

    [KnownBugFact("X-1")]
    public Task PositionalRecords() => RoundTripScenarios.PositionalRecords(_harness);

    [Fact]
    public Task Volume() => RoundTripScenarios.Volume(_harness);

    [Fact]
    public Task Empty() => RoundTripScenarios.Empty(_harness);

    [KnownBugFact("CSV-1")]
    public async Task Source_maps_pascal_case_headers_written_by_other_tools()
    {
        _harness.Provider.Put(_harness.Uri, Encoding.UTF8.GetBytes("Id,Name,Amount\n1,Ada,12.5\n"));

        var rows = await _harness.ReadAsync<ScalarRecord>();

        rows.Should().ContainSingle().Which.Should().BeEquivalentTo(new { Id = 1, Name = "Ada", Amount = 12.5m });
    }

    [KnownBugFact("CSV-2")]
    public async Task Source_rejects_unparseable_values_instead_of_defaulting()
    {
        _harness.Provider.Put(_harness.Uri, Encoding.UTF8.GetBytes("id,amount\n1,not-a-number\n"));

        var read = () => _harness.ReadAsync<ScalarRecord>();

        await read.Should().ThrowAsync<Exception>();
    }

    [KnownBugFact("CSV-3")]
    public async Task Source_honours_explicit_delimiter_without_mutating_configuration()
    {
        var configuration = new CsvConfiguration();
        configuration.HelperConfiguration.Delimiter = ";";
        var harness = new CsvHarness { Configuration = () => configuration };
        harness.Provider.Put(harness.Uri, Encoding.UTF8.GetBytes("id;name\n1;a,b\n"));

        var rows = await harness.ReadAsync<ScalarRecord>();

        rows.Should().ContainSingle().Which.Name.Should().Be("a,b");
        configuration.HelperConfiguration.DetectDelimiter.Should().BeFalse();
    }

    [KnownBugFact("CSV-4")]
    public async Task Sink_writes_scalar_struct_items_as_a_single_column()
    {
        DateTime[] items = [new(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc)];

        await _harness.WriteAsync(items);

        var lines = Encoding.UTF8.GetString(_harness.Provider.Get(_harness.Uri)).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        lines.Should().ContainSingle().Which.Should().NotContain(",");
    }
}
