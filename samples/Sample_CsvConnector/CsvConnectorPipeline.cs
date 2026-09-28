using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Csv;
using NPipeline.Connectors.Errors;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using Sample_CsvConnector.Nodes;

namespace Sample_CsvConnector;

/// <summary>
///     CSV Connector pipeline that demonstrates reading from a CSV file, validating and transforming data,
///     and writing processed data to another CSV file.
/// </summary>
/// <remarks>
///     This pipeline implements a complete CSV processing workflow:
///     1. CsvSourceNode reads customer data from a source CSV file
///     2. ValidationTransform validates customer records and filters invalid ones
///     3. DataTransform enriches and normalizes customer data
///     4. CsvSinkNode writes the processed data to a target CSV file
/// </remarks>
public class CsvConnectorPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <remarks>
    ///     This method creates a simple CSV processing pipeline:
    ///     CsvSourceNode -> ValidationTransform -> DataTransform -> CsvSinkNode
    ///     The pipeline reads customer data from the input CSV file, validates and transforms it,
    ///     then writes the processed records to an output CSV file.
    ///     Local files need no storage configuration: the nodes resolve the file system provider from the URI.
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Define paths for source and output files
        var sourcePath = GetSourcePath();
        var targetPath = GetTargetPath();

        // Columns bind to Customer's members by name, case-insensitively, once per file. [Column] and [IgnoreColumn]
        // control the mapping; values convert strictly, so a bad value fails the run (or goes to RowErrorHandler).
        var sourceNode = CsvConnector.Source<Customer>(
            StorageUri.FromFilePath(sourcePath),
            options => options with
            {
                // Log and skip rows that do not convert instead of failing the whole run.
                RowErrorHandler = error =>
                {
                    Console.WriteLine($"Skipping row {error.RecordNumber}: {error.Exception.Message}");
                    return RowErrorAction.Skip;
                },
            });

        // For full control, map each row by hand instead:
        // CsvConnector.Source(uri, row => new Customer { Id = row.Get<int>("Id"), FirstName = row.Get<string>("FirstName") });

        var source = builder.AddSource(sourceNode, "csv-source");

        // Add validation transform to filter invalid records
        var validation = builder.AddTransform<ValidationTransform, Customer, Customer>("validation-transform");

        // Add data transform to enrich and normalize records
        var transform = builder.AddTransform<DataTransform, Customer, Customer>("data-transform");

        // Writes the processed customers: one column per member in declaration order, skipping [IgnoreColumn] members.
        // On the file system the file is written under a temporary name and moved into place when complete.
        var sinkNode = CsvConnector.Sink<Customer>(StorageUri.FromFilePath(targetPath));
        var sink = builder.AddSink(sinkNode, "csv-sink");

        // Connect nodes in sequence: source -> validation -> transform -> sink
        builder.Connect(source, validation);
        builder.Connect(validation, transform);
        builder.Connect(transform, sink);

        // Log pipeline configuration
        var logger = context.Observability.LoggerFactory.CreateLogger("CsvConnectorPipeline");
        logger.Log(LogLevel.Information, "CSV pipeline configured: {SourcePath} -> {TargetPath}", sourcePath, targetPath);
    }

    /// <summary>
    ///     Gets the source CSV file path.
    /// </summary>
    private static string GetSourcePath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "customers.csv");
    }

    /// <summary>
    ///     Gets the target CSV file path for processed data.
    /// </summary>
    private static string GetTargetPath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "processed_customers.csv");
    }

    /// <summary>
    ///     Finds the project directory by looking for the .csproj file.
    /// </summary>
    private static string FindProjectDirectory(string startDirectory)
    {
        var directory = new DirectoryInfo(startDirectory);

        // Navigate up the directory tree looking for the .csproj file
        while (directory != null)
        {
            if (directory.GetFiles("*.csproj").FirstOrDefault() != null)
                return directory.FullName;

            directory = directory.Parent;
        }

        return startDirectory;
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    public static string GetDescription() =>
        @"CSV Connector Pipeline Sample:

This sample demonstrates CSV data processing with NPipeline:

WHAT IT DOES:
- Reads customer records from a CSV file (customers.csv)
- Validates each record and filters out invalid ones
- Transforms and enriches the valid records
- Writes the processed records to a new CSV file (processed_customers.csv)

PIPELINE FLOW:
CsvSourceNode (read)
  → ValidationTransform (filter invalid records)
    → DataTransform (enrich/normalize)
      → CsvSinkNode (write)

KEY FEATURES:
- Simple file-based CSV processing using StorageUri
- Data validation with filtering
- Data transformation and enrichment
- Built-in error handling and logging
- No complex configuration needed - just specify the file paths

GETTING STARTED:
Create the nodes with CsvConnector.Source and CsvConnector.Sink, add them
to the builder, and connect them. Options are records: adjust them with
'with' expressions (delimiter, culture, row error handling).

This is one of the simplest ways to process CSV files in NPipeline!";
}
