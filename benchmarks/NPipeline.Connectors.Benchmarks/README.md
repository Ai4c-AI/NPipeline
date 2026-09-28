# Connector benchmarks

Read and write benchmarks for the CSV, JSON, Excel, Parquet and HTTP connectors. Use them to measure connector
performance work against the committed baselines in [`baselines/`](baselines/).

Every benchmark reads or writes the same 20-column `WideRecord` (integers, longs, doubles, decimals, strings, dates, a
flag and a GUID), so results are comparable across connectors. File connectors use the in-memory storage provider from
`NPipeline.Tests.Common`, and HTTP uses in-process handlers. The numbers therefore measure parsing, mapping and
serialisation, not disk or network I/O.

| Benchmark | Rows | What it measures |
| --- | --- | --- |
| `CsvBenchmarks.Write` / `Read` | 100,000 | Attribute-mapped CSV sink and source |
| `JsonBenchmarks.Write` / `Read` | 100,000 | JSON sink and source, for both `Array` and `NewlineDelimited` |
| `ExcelBenchmarks.Write` / `Read` | 25,000 | XLSX sink and source (fewer rows because the current writer is slow) |
| `ParquetBenchmarks.Write` / `Read` | 100,000 | Parquet sink and source with default row groups |
| `ParquetBenchmarks.ReadThreeColumns` | 100,000 | Reading 3 of 20 columns with `ProjectedColumns` and a row mapper |
| `HttpBenchmarks.SourcePaged` | 100,000 | Offset pagination over 100 root-array pages of 1,000 items |
| `HttpBenchmarks.SinkBatched` | 100,000 | POSTs in batches of 100 |

## Run

Run from this directory, in Release mode, on a quiet machine:

```shell
dotnet run -c Release -f net10.0 -- --filter '*'
```

To run one connector, filter by class name, for example `--filter '*Parquet*'`. Use `-f net8.0` to measure the LTS
target. Results are written to `BenchmarkDotNet.Artifacts/results/` as GitHub markdown and full JSON.

The **Connector Benchmarks** GitHub workflow runs the same command on demand (`workflow_dispatch`) and uploads the
results as an artifact.

## Update the baselines

After a change that is meant to improve performance:

1. Run the affected benchmarks on the same machine as the existing baseline.
2. Compare them with the table in `baselines/`.
3. Add a new baseline file named `YYYY-MM-DD-<commit>.md`. Keep the old one, so the history of gains stays visible.
