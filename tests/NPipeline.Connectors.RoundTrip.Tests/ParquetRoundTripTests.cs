using NPipeline.Connectors.Parquet;
using NPipeline.Connectors.RoundTrip.Tests.Harnesses;
using NPipeline.Connectors.RoundTrip.Tests.Infrastructure;
using NPipeline.Connectors.RoundTrip.Tests.Models;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.RoundTrip.Tests;

public sealed class ParquetRoundTripTests
{
    private readonly ParquetHarness _harness = new();

    [Fact]
    public Task Scalars() => RoundTripScenarios.Scalars(_harness);

    [KnownBugFact("PQ-4")]
    public Task SmallAndUnsignedIntegers() => RoundTripScenarios.SmallAndUnsignedIntegers(_harness);

    [Fact]
    public Task Nullables() => RoundTripScenarios.Nullables(_harness);

    [KnownBugFact("PQ-3")]
    public Task Enums() => RoundTripScenarios.Enums(_harness);

    [KnownBugFact("PQ-2")]
    public Task DateTimeOffsets() => RoundTripScenarios.DateTimeOffsets(_harness);

    [Fact]
    public Task DateOnlys() => RoundTripScenarios.DateOnlys(_harness);

    [Fact]
    public Task Text() => RoundTripScenarios.Text(_harness);

    [Fact]
    public Task ControlCharacters() => RoundTripScenarios.ControlCharacters(_harness);

    [KnownBugFact("PQ-10")]
    public Task Binary() => RoundTripScenarios.Binary(_harness);

    [KnownBugFact("PQ-5")]
    public Task Lists() => RoundTripScenarios.Lists(_harness);

    [KnownBugFact("X-1")]
    public Task PositionalRecords() => RoundTripScenarios.PositionalRecords(_harness);

    [Fact]
    public Task Volume() => RoundTripScenarios.Volume(_harness);

    [Fact]
    public Task Empty() => RoundTripScenarios.Empty(_harness);

    [KnownBugFact("PQ-12")]
    public Task Reads_from_non_seekable_streams() =>
        RoundTripScenarios.Scalars(new ParquetHarness { Provider = new InMemoryStorageProvider { NonSeekableReads = true } });

    [Fact]
    public Task Writes_to_non_seekable_streams() =>
        RoundTripScenarios.Scalars(new ParquetHarness { Provider = new InMemoryStorageProvider { NonSeekableWrites = true } });

    [Fact]
    public async Task Atomic_write_leaves_no_temporary_objects_behind()
    {
        await _harness.WriteAsync([ScalarRecord.Create(1)]);

        _harness.Provider.Keys.Should().ContainSingle().Which.Should().EndWith("/data.parquet");
    }

    [KnownBugFact("PQ-1")]
    public async Task Parallel_directory_read_does_not_deadlock()
    {
        // The deadlock depends on thread-pool scheduling, so read several times; one pass often gets lucky.
        const int files = 40;
        const int rowsPerFile = 200;
        var provider = new InMemoryStorageProvider();
        var configuration = new ParquetConfiguration { RowGroupSize = 10, FileReadParallelism = 4, UseAtomicWrite = false };

        for (var file = 0; file < files; file++)
        {
            var sink = new ParquetSinkNode<ScalarRecord>(provider, InMemoryStorageProvider.Uri($"parts/f{file:D2}.parquet"), configuration);
            await NodeRunner.WriteAsync(sink, Enumerable.Range(file * rowsPerFile, rowsPerFile).Select(ScalarRecord.Create));
        }

        for (var attempt = 0; attempt < 5; attempt++)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var source = new ParquetSourceNode<ScalarRecord>(provider, InMemoryStorageProvider.Uri("parts/"), configuration);

            var rows = await NodeRunner.ReadAsync(source, timeout.Token);

            rows.Select(r => r.Id).Should().Equal(Enumerable.Range(0, files * rowsPerFile), "files are read in parallel but emitted in file order");
        }
    }
}
