# NPipeline DuckDB Connector

Source and sink nodes for DuckDB in NPipeline pipelines: queries over DuckDB databases and over Parquet, CSV and JSON
files, in process.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.DuckDB
```

Targets .NET 8.0, 9.0 and 10.0.

## Features

- **Name-based binding**: columns bind to members case-insensitively, once per query, through a compiled mapper that
  reads each value typed at its ordinal. Positional records, `init` accessors and `required` members work.
- **Strict conversion**: a value that does not fit its member is a row error, never a silent default; row errors can
  fail the read, be skipped, or go to the pipeline's dead-letter sink.
- **Transactions per batch** by default, so a batch lands whole; or one transaction for the whole write.
- **Failed batches** fail the write, or go to the dead-letter sink with their items.
- **Upserts** on key columns, and identifiers that are always quoted and escaped.
- **Queries over files**: `DuckDBConnector.FromFile<T>("data/*.parquet")`, or any `read_parquet`/`read_csv` query.
- **Typed appender writes**, tables created from the record, and export to Parquet, CSV or JSON files.
- **No native setup**: depends on `DuckDB.NET.Data.Full`, which brings DuckDB's native binaries with it.

## Usage

```csharp
using NPipeline.Connectors.DuckDB;

var readings = DuckDBConnector.FromFile<SensorReading>("data/readings/*.parquet");
var sink = DuckDBConnector.Sink<SensorReading>(DuckDBDatabase.File("sensors.duckdb"), "readings");
var export = DuckDBConnector.ToFile<SensorReading>("out/readings.parquet");
```

See the [DuckDB connector documentation](https://docs.npipeline.net/connectors/duckdb) and
[SQL Connectors: Shared Behaviour](https://docs.npipeline.net/connectors/sql-connectors) for every option.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.
