using NPipeline.Connectors.Csv;
using NPipeline.Connectors.Excel;
using NPipeline.Connectors.Json;
using NPipeline.Connectors.Parquet;
using NPipeline.Nodes;

namespace NPipeline.Connectors.RoundTrip.Tests.Harnesses;

public sealed class CsvHarness() : FileConnectorHarness(".csv")
{
    public Func<CsvReadOptions, CsvReadOptions> ReadOptions { get; init; } = o => o;

    public Func<CsvWriteOptions, CsvWriteOptions> WriteOptions { get; init; } = o => o;

    protected override SinkNode<T> CreateSink<T>() => CsvConnector.Sink<T>(Uri, o => WriteOptions(o with { Provider = Provider }));

    protected override SourceNode<T> CreateSource<T>() => CsvConnector.Source<T>(Uri, o => ReadOptions(o with { Provider = Provider }));
}

public sealed class JsonHarness(JsonFormat format) : FileConnectorHarness(format == JsonFormat.Array ? ".json" : ".ndjson")
{
    public JsonFormat Format { get; } = format;

    protected override SinkNode<T> CreateSink<T>() => JsonConnector.Sink<T>(Uri, o => o with { Provider = Provider, Format = Format });

    protected override SourceNode<T> CreateSource<T>() => JsonConnector.Source<T>(Uri, o => o with { Provider = Provider, Format = Format });
}

public sealed class ExcelHarness() : FileConnectorHarness(".xlsx")
{
    protected override SinkNode<T> CreateSink<T>() => ExcelConnector.Sink<T>(Uri, o => o with { Provider = Provider });

    protected override SourceNode<T> CreateSource<T>() => ExcelConnector.Source<T>(Uri, o => o with { Provider = Provider });
}

public sealed class ParquetHarness() : FileConnectorHarness(".parquet")
{
    /// <summary>Small row groups by default, so round trips cross row-group boundaries.</summary>
    public Func<ParquetConfiguration> Configuration { get; init; } = () => new ParquetConfiguration { RowGroupSize = 1_000 };

    protected override SinkNode<T> CreateSink<T>() => new ParquetSinkNode<T>(Provider, Uri, Configuration());

    protected override SourceNode<T> CreateSource<T>() => new ParquetSourceNode<T>(Provider, Uri, Configuration());
}
