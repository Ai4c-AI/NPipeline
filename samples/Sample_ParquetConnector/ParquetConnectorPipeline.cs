using NPipeline.Connectors.Parquet;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using Parquet;
using Sample_ParquetConnector.Nodes;

namespace Sample_ParquetConnector;

/// <summary>
///     Demonstrates writing to and reading from a local Parquet file.
///     Pipeline: SalesDataSourceNode → ParquetSinkNode
/// </summary>
public sealed class ParquetConnectorPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(new SalesDataSourceNode(), "sales-source");
        // Each record's members become typed columns; Zstd trades a little CPU for smaller files than the Snappy default.
        var sink = builder.AddSink(
            ParquetConnector.Sink<SalesRecord>(StorageUri.FromFilePath(GetOutputPath()), o => o with { Codec = CompressionMethod.Zstd }),
            "parquet-sink");

        builder.Connect(source, sink);
    }

    public static string GetOutputPath()
    {
        var dir = Path.Combine(Directory.GetCurrentDirectory(), "output");
        Directory.CreateDirectory(dir);
        return Path.Combine(dir, "sales.parquet");
    }
}
