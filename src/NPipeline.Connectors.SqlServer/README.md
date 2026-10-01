# NPipeline SQL Server Connector

Source and sink nodes for SQL Server in NPipeline pipelines, on NPipeline's shared SQL layer.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.SqlServer
```

Targets .NET 8.0, 9.0 and 10.0.

## Features

- **Name-based binding**: columns bind to members case-insensitively, once per query, through a compiled mapper that
  reads each value typed at its ordinal. Positional records, `init` accessors and `required` members work.
- **Strict conversion**: a value that does not fit its member is a row error, never a silent default; row errors can
  fail the read, be skipped, or go to the pipeline's dead-letter sink.
- **Transactions per batch** by default, so a batch lands whole and a transient failure is retried safely; or one
  transaction for the whole write.
- **Failed batches** fail the write, or go to the dead-letter sink with their items.
- **Upserts** on key columns, and identifiers that are always quoted and escaped.
- **Three write strategies**: multi-row `INSERT` statements under the 2,100-parameter limit, one statement per row, or
  `SqlBulkCopy` streamed from a data reader.
- **`MERGE … WITH (HOLDLOCK)` upserts**, identity columns left out of writes, and parameters typed for plan reuse.
- **A build-time analyzer** (NP9502) that warns when a checkpointing query has no `ORDER BY`, included in this package.

## Usage

```csharp
using NPipeline.Connectors.SqlServer;

var source = SqlServerConnector.Source<Order>(connectionString, "SELECT * FROM dbo.Orders ORDER BY Id");
var sink = SqlServerConnector.Sink<Order>(connectionString, "Orders", o => o with
{
    WriteStrategy = SqlServerWriteStrategy.BulkCopy,
    BatchSize = 10_000,
});
```

See the [SQL Server connector documentation](https://docs.npipeline.net/connectors/sqlserver) and
[SQL Connectors: Shared Behaviour](https://docs.npipeline.net/connectors/sql-connectors) for every option.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.
