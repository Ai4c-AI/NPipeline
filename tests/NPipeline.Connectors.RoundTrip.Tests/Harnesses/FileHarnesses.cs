using NPipeline.Connectors.Csv;
using NPipeline.Connectors.Excel;
using NPipeline.Connectors.Json;
using NPipeline.Connectors.Parquet;
using NPipeline.Nodes;

namespace NPipeline.Connectors.RoundTrip.Tests.Harnesses;

public sealed class CsvHarness() : FileConnectorHarness(".csv")
{
    public Func<CsvConfiguration> Configuration { get; init; } = () => new CsvConfiguration();

    protected override SinkNode<T> CreateSink<T>() => new CsvSinkNode<T>(Provider, Uri, Configuration());

    protected override SourceNode<T> CreateSource<T>() => new CsvSourceNode<T>(Provider, Uri, Configuration());
}

public sealed class JsonHarness(JsonFormat format) : FileConnectorHarness(format == JsonFormat.Array ? ".json" : ".ndjson")
{
    public JsonFormat Format { get; } = format;

    protected override SinkNode<T> CreateSink<T>() => new JsonSinkNode<T>(Provider, Uri, new JsonConfiguration { Format = Format });

    protected override SourceNode<T> CreateSource<T>() => new JsonSourceNode<T>(Provider, Uri, new JsonConfiguration { Format = Format });
}

public sealed class ExcelHarness() : FileConnectorHarness(".xlsx")
{
    protected override SinkNode<T> CreateSink<T>() => new ExcelSinkNode<T>(Provider, Uri, new ExcelConfiguration());

    protected override SourceNode<T> CreateSource<T>() => new ExcelSourceNode<T>(Provider, Uri, new ExcelConfiguration());
}

public sealed class ParquetHarness() : FileConnectorHarness(".parquet")
{
    /// <summary>Small row groups by default, so round trips cross row-group boundaries.</summary>
    public Func<ParquetConfiguration> Configuration { get; init; } = () => new ParquetConfiguration { RowGroupSize = 1_000 };

    protected override SinkNode<T> CreateSink<T>() => new ParquetSinkNode<T>(Provider, Uri, Configuration());

    protected override SourceNode<T> CreateSource<T>() => new ParquetSourceNode<T>(Provider, Uri, Configuration());
}
