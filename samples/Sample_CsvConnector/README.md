# Sample 11: CSV Connector

This sample reads customer records from a CSV file, validates and normalizes them, and writes the result to another CSV
file with the `NPipeline.Connectors.Csv` connector.

## What it demonstrates

- Creating a CSV source and sink with `CsvConnector.Source<T>` and `CsvConnector.Sink<T>`.
- Mapping columns to a class with `[Column]` and `[IgnoreColumn]`.
- Skipping rows that do not convert with a `RowErrorHandler`.
- Validation and enrichment in custom transform nodes.

The pipeline is:

```text
CsvSourceNode<Customer> -> ValidationTransform -> DataTransform -> CsvSinkNode<Customer>
```

## Running the sample

Requires the .NET 10 SDK.

```bash
cd samples/Sample_CsvConnector
dotnet run
```

The pipeline reads `Data/customers.csv` and writes `Data/processed_customers.csv`. The program then prints the output
file's size and its first five lines:

```text
Sample output (first 5 lines):
  Id,FirstName,LastName,Email,Age,RegistrationDate,Country
  1,John,Doe,john.doe@example.com,28,2023-01-15T00:00:00.0000000Z,United States
  2,Jane,Smith,jane.smith@example.com,34,2023-02-20T00:00:00.0000000Z,Canada
  3,Bob,Johnson,bob.johnson@example.com,45,2023-03-10T00:00:00.0000000Z,United Kingdom
  4,Alice,Williams,alice.williams@example.com,29,2023-04-05T00:00:00.0000000Z,Australia
  ... and 9 more lines
```

## How it works

### Reading

```csharp
var sourceNode = CsvConnector.Source<Customer>(
    StorageUri.FromFilePath(sourcePath),
    options => options with
    {
        RowErrorHandler = error =>
        {
            Console.WriteLine($"Skipping row {error.RecordNumber}: {error.Exception.Message}");
            return RowErrorAction.Skip;
        },
    });
```

The header binds to `Customer`'s members once per file. Header names match member names case-insensitively, so
`Id`, `id` and `ID` all map to `Id`. Columns the class does not have are ignored.

Values convert strictly and culture-invariantly. A value that does not convert, such as `abc` in the `Age` column, is a
row error. By default a row error fails the run. This sample's handler logs the row and skips it. The handler can also
return `RowErrorAction.Fail` or `RowErrorAction.DeadLetter`. `RowErrorAction` is in `NPipeline.Connectors.Errors`.

Options are immutable records. Change them with a `with` expression, for example
`o => o with { Delimiter = "\t" }` for a TSV file.

Local files need no storage configuration. The node resolves the file system provider from the URI.

### Mapping

`Customer` uses the shared attributes from `NPipeline.Connectors.Attributes`:

```csharp
public class Customer
{
    [Column("Id")]
    public int Id { get; set; }

    [Column("FirstName")]
    public string FirstName { get; set; } = string.Empty;

    // LastName, Email, Age, RegistrationDate and Country follow the same pattern.

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

`[Column("name")]` sets the column name. `[IgnoreColumn]` leaves a member out of reading and writing. In this sample the
`[Column]` names match the member names, so the attributes only make the mapping explicit. Use them when the file's
headers differ from your member names, for example `[Column("cust_id")]`.

To map rows by hand instead, pass a mapper. `CsvRow.Get<T>` converts strictly and names the column when it fails:

```csharp
var sourceNode = CsvConnector.Source(
    StorageUri.FromFilePath(sourcePath),
    row => new Customer
    {
        Id = row.Get<int>("Id"),
        FirstName = row.Get<string>("FirstName"),
    });
```

### Writing

```csharp
var sinkNode = CsvConnector.Sink<Customer>(StorageUri.FromFilePath(targetPath));
```

The sink writes a header, then one row per customer. Columns follow the members' declaration order and skip
`[IgnoreColumn]` members. Header names are the `[Column]` names, or the member names as they are when there is no
attribute. Dates are written in ISO 8601 round-trip form, which is why `2023-01-15` in the input becomes
`2023-01-15T00:00:00.0000000Z` in the output. On the file system, the sink writes to a temporary file and moves it into
place when it finishes.

### Transforms

`ValidationTransform` checks each record:

- `Id` is greater than 0.
- `FirstName`, `LastName` and `Country` are not empty.
- `Email` is present and looks like an email address.
- `Age` is between 0 and 150.
- `RegistrationDate` is set and not in the future.

It logs a warning for each failure. It is created with `filterInvalidRecords = true`, so an invalid record throws an
`InvalidOperationException`.

`DataTransform` returns a normalized copy of each record:

- Trims names and capitalizes the first letter.
- Trims email addresses and makes them lowercase.
- Expands `USA` and `UK` to `United States` and `United Kingdom`, and title-cases other country names.

## Sample data

`Data/customers.csv` has 13 customer records. All of them pass validation, so the output has 13 rows.

## Project structure

```text
Sample_CsvConnector/
├── Data/
│   ├── customers.csv              # Input
│   └── processed_customers.csv    # Output, overwritten on each run
├── Nodes/
│   ├── DataTransform.cs           # Normalizes names, email and country
│   └── ValidationTransform.cs     # Validates each record
├── Customer.cs                    # Record type with [Column] and [IgnoreColumn]
├── CsvConnectorPipeline.cs        # Pipeline definition
├── Program.cs                     # Host setup and pipeline run
└── Sample_CsvConnector.csproj
```

## Learn more

- [CSV Connector](../../docs/connectors/csv.md)
- [File Connectors: Shared Behaviour](../../docs/connectors/file-connectors.md)
