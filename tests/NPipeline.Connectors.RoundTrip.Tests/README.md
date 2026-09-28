# Connector round-trip tests

These tests write records through each connector's sink, read them back through its source, and expect the same
records in the same order. They cover the CSV, JSON (array and NDJSON), Excel, Parquet and HTTP connectors, and add
targeted tests for the bugs found in the connector review.

## Hostile environment

A module initializer (`Infrastructure/HostileEnvironment.cs`) runs every test with:

- the time zone set to `Australia/Brisbane` (UTC+10, no daylight saving), and
- the culture set to `de-DE`, which uses a comma as the decimal separator.

CI runners default to UTC and `en-US`, which hides time-zone and culture bugs. Two guard tests fail if the environment
is not applied. On Windows, the `TZ` variable is ignored, so the time-zone guard is skipped there.

## Known bugs

A test that asserts correct behaviour but fails today is marked with `[KnownBugFact("ID")]`, where the ID refers to
the connector review (for example `CSV-1` or `PQ-2`). These tests are skipped by default, so CI stays green.

To run them, set `NPIPELINE_RUN_KNOWN_BUGS=1`:

```shell
NPIPELINE_RUN_KNOWN_BUGS=1 dotnet test tests/NPipeline.Connectors.RoundTrip.Tests
```

When you fix a bug, change its tests from `[KnownBugFact("ID")]` to `[Fact]` in the same change. To find them, search
for the ID:

```shell
grep -rn 'KnownBugFact("PQ-2")' tests/NPipeline.Connectors.RoundTrip.Tests
```

`Parallel_directory_read_does_not_deadlock` (`PQ-1`) depends on thread-pool scheduling. While the bug is open, it
occasionally passes under full-suite load. After the fix, it must pass every time.

## Layout

| Path | Contents |
| --- | --- |
| `Models/RoundTripModels.cs` | One model per type family (scalars, nullables, enums, dates, text, binary, lists, nested, records) |
| `RoundTripScenarios.cs` | The shared write-then-read scenarios and their assertions |
| `Harnesses/` | One harness per connector. File connectors use `InMemoryStorageProvider` from `NPipeline.Tests.Common`; HTTP uses an in-process `FakeJsonApi` handler |
| `*RoundTripTests.cs` | One class per connector, listing the scenarios its format supports plus connector-specific bug tests |

`InMemoryStorageProvider` can make read or write streams non-seekable (`NonSeekableReads`, `NonSeekableWrites`),
which reproduces object-store streams such as S3's.

## Adding a scenario

1. Add a model to `Models/RoundTripModels.cs` if no existing model covers the types.
2. Add a scenario method to `RoundTripScenarios`.
3. Call it from each connector class whose format can represent those types. If a format cannot represent them (CSV
   cannot tell a null `byte[]` from an empty one, for example), leave the scenario out of that class and say why in a
   comment.
