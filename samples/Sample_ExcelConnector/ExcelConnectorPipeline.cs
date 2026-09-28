using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Excel;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using Sample_ExcelConnector.Nodes;

namespace Sample_ExcelConnector;

/// <summary>
///     Excel Connector pipeline that demonstrates reading from an Excel file, validating and transforming data,
///     and writing processed data to another Excel file.
/// </summary>
/// <remarks>
///     This pipeline implements a complete Excel processing workflow:
///     1. ExcelSourceNode reads customer data from a source Excel file
///     2. ValidationTransform validates customer records and logs the invalid ones
///     3. DataTransform enriches and normalizes customer data
///     4. ExcelSinkNode writes the processed data to a target Excel file
/// </remarks>
public class ExcelConnectorPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <remarks>
    ///     This method creates a simple Excel processing pipeline:
    ///     ExcelSourceNode -> ValidationTransform -> DataTransform -> ExcelSinkNode
    ///     The pipeline reads customer data from the input Excel file, validates and transforms it,
    ///     then writes the processed records to an output Excel file.
    ///     Local files need no storage configuration: the nodes resolve the file system provider from the URI.
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Define paths for source and output files
        var sourcePath = GetSourcePath();
        var targetPath = GetTargetPath();

        // Columns bind to Customer's members by header, case-insensitively; cells convert strictly, so a text value in
        // a number column is an error rather than a silent zero. Rows that fail are logged and skipped.
        var sourceNode = ExcelConnector.Source<Customer>(
            StorageUri.FromFilePath(sourcePath),
            options => options with
            {
                RowErrorHandler = error =>
                {
                    Console.WriteLine($"Skipping row {error.RecordNumber}: {error.Exception.Message}");
                    return RowErrorAction.Skip;
                },
            });

        var source = builder.AddSource(sourceNode, "excel-source");

        // Add validation transform to log invalid records (the sample data has some on purpose)
        var validation = builder.AddTransform<ValidationTransform, Customer, Customer>("validation-transform");

        // Add data transform to enrich and normalize records
        var transform = builder.AddTransform<DataTransform, Customer, Customer>("data-transform");

        // The sink streams the workbook: a bold, frozen header with filter buttons, and dates formatted as dates.
        var sinkNode = ExcelConnector.Sink<Customer>(
            StorageUri.FromFilePath(targetPath),
            options => options with { FreezeHeader = true, AutoFilter = true });
        var sink = builder.AddSink(sinkNode, "excel-sink");

        // Connect nodes in sequence: source -> validation -> transform -> sink
        builder.Connect(source, validation);
        builder.Connect(validation, transform);
        builder.Connect(transform, sink);

        // Log pipeline configuration
        var logger = context.Observability.LoggerFactory.CreateLogger("ExcelConnectorPipeline");
        logger.Log(LogLevel.Information, "Excel pipeline configured: {SourcePath} -> {TargetPath}", sourcePath, targetPath);
    }

    /// <summary>
    ///     Gets the source Excel file path.
    /// </summary>
    private static string GetSourcePath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "customers.xlsx");
    }

    /// <summary>
    ///     Gets the target Excel file path for processed data.
    /// </summary>
    private static string GetTargetPath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "processed_customers.xlsx");
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
        @"Excel Connector Pipeline Sample:

This sample demonstrates Excel data processing with NPipeline:

WHAT IT DOES:
- Reads customer records from an Excel file (customers.xlsx)
- Validates each record and logs a warning for invalid ones
- Transforms and enriches the valid records
- Writes the processed records to a new Excel file (processed_customers.xlsx)

PIPELINE FLOW:
ExcelSourceNode (read)
  → ValidationTransform (log invalid records)
    → DataTransform (enrich/normalize)
      → ExcelSinkNode (write)

KEY FEATURES:
- Simple file-based Excel processing using StorageUri
- Data validation with warnings
- Data transformation and enrichment
- Built-in error handling and logging
- No complex configuration needed - just specify the file paths

GETTING STARTED:
The pipeline is straightforward - create node instances with file paths,
add them to the builder, and connect them. Create the nodes with
ExcelConnector.Source and ExcelConnector.Sink; options are records
adjusted with 'with' (sheet name, header row, frozen header, filters).

This is one of the simplest ways to process Excel files in NPipeline!";
}
