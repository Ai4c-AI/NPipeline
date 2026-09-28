using BenchmarkDotNet.Attributes;
using NPipeline.Connectors.Csv;
using NPipeline.Connectors.Excel;
using NPipeline.Connectors.Json;
using NPipeline.Connectors.Parquet;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.Benchmarks.Benchmarks;

/// <summary>
///     Shared setup for file connectors. Files live in <see cref="InMemoryStorageProvider" />, so the numbers measure
///     parsing, mapping and serialisation rather than the disk. The read input is written once by the connector's own
///     sink, and every write benchmark writes the same records.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 2, iterationCount: 8)]
public abstract class FileConnectorBenchmark
{
    protected InMemoryStorageProvider Provider { get; } = new();

    protected FileConnectorBenchmark(string extension)
    {
        // The extension matters: the Parquet source treats a URI without one as a directory to list.
        ReadUri = InMemoryStorageProvider.Uri($"input{extension}");
        WriteUri = InMemoryStorageProvider.Uri($"output{extension}");
    }

    protected StorageUri ReadUri { get; }

    protected StorageUri WriteUri { get; }

    protected WideRecord[] Records { get; private set; } = [];

    /// <summary>Row count per operation; each record has 20 columns.</summary>
    protected abstract int Rows { get; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        Records = WideRecord.Generate(Rows);
        await WriteAsync(ReadUri);

        // A benchmark that silently reads nothing reports microseconds; refuse to measure it.
        var read = await ReadAsync(ReadUri);

        if (read != Rows)
            throw new InvalidOperationException($"{GetType().Name} read {read} of {Rows} rows during setup.");
    }

    [Benchmark]
    public Task Write() => WriteAsync(WriteUri);

    [Benchmark]
    public Task<int> Read() => ReadAsync(ReadUri);

    protected abstract Task WriteAsync(StorageUri uri);

    protected abstract Task<int> ReadAsync(StorageUri uri);
}

public class CsvBenchmarks() : FileConnectorBenchmark(".csv")
{
    protected override int Rows => 100_000;

    protected override Task WriteAsync(StorageUri uri) => NodeRunner.WriteAsync(CsvConnector.Sink<WideRecord>(uri, o => o with { Provider = Provider }), Records);

    protected override Task<int> ReadAsync(StorageUri uri) => NodeRunner.ReadAsync(CsvConnector.Source<WideRecord>(uri, o => o with { Provider = Provider }));
}

public class JsonBenchmarks() : FileConnectorBenchmark(".json")
{
    protected override int Rows => 100_000;

    [Params(JsonFormat.Array, JsonFormat.NewlineDelimited)]
    public JsonFormat Format { get; set; }

    protected override Task WriteAsync(StorageUri uri) =>
        NodeRunner.WriteAsync(JsonConnector.Sink<WideRecord>(uri, o => o with { Provider = Provider, Format = Format }), Records);

    protected override Task<int> ReadAsync(StorageUri uri) =>
        NodeRunner.ReadAsync(JsonConnector.Source<WideRecord>(uri, o => o with { Provider = Provider, Format = Format }));
}

public class ExcelBenchmarks() : FileConnectorBenchmark(".xlsx")
{
    /// <summary>Fewer rows than the other formats: the current OpenXML writer takes seconds per 100,000 rows.</summary>
    protected override int Rows => 25_000;

    protected override Task WriteAsync(StorageUri uri) => NodeRunner.WriteAsync(ExcelConnector.Sink<WideRecord>(uri, o => o with { Provider = Provider }), Records);

    protected override Task<int> ReadAsync(StorageUri uri) => NodeRunner.ReadAsync(ExcelConnector.Source<WideRecord>(uri, o => o with { Provider = Provider }));
}

public class ParquetBenchmarks() : FileConnectorBenchmark(".parquet")
{
    private static readonly ParquetConfiguration Configuration = new() { UseAtomicWrite = false };

    private static readonly ParquetConfiguration ProjectedConfiguration = new()
    {
        UseAtomicWrite = false,
        ProjectedColumns = [nameof(WideRecord.Id), nameof(WideRecord.Name), nameof(WideRecord.Total)],
    };

    protected override int Rows => 100_000;

    /// <summary>Three of twenty columns: the case that projection (PQ-P3) and typed columnar mapping (PQ-P1) speed up.</summary>
    [Benchmark]
    public Task<int> ReadThreeColumns() =>
        NodeRunner.ReadAsync(new ParquetSourceNode<(int, string, decimal)>(
            Provider,
            ReadUri,
            row => (row.Get<int>(nameof(WideRecord.Id)), row.Get<string>(nameof(WideRecord.Name)), row.Get<decimal>(nameof(WideRecord.Total))),
            ProjectedConfiguration));

    protected override Task WriteAsync(StorageUri uri) => NodeRunner.WriteAsync(new ParquetSinkNode<WideRecord>(Provider, uri, Configuration), Records);

    protected override Task<int> ReadAsync(StorageUri uri) => NodeRunner.ReadAsync(new ParquetSourceNode<WideRecord>(Provider, uri, Configuration));
}
