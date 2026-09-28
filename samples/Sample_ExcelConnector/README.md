# Sample 12: Excel Connector

This sample reads customer records from an Excel workbook, validates and normalizes them, and writes the result to
another workbook with the `NPipeline.Connectors.Excel` connector.

## What it demonstrates

- Creating an Excel source and sink with `ExcelConnector.Source<T>` and `ExcelConnector.Sink<T>`.
- Binding columns to a class by header name, with typed cells: numbers, booleans and dates.
- Skipping rows that do not convert with a `RowErrorHandler`.
- Writing a workbook with a frozen header and filter buttons.
- Validation and enrichment in custom transform nodes.

The pipeline is:

```text
ExcelSourceNode<Customer> -> ValidationTransform -> DataTransform -> ExcelSinkNode<Customer>
```

## Running the sample

Requires the .NET 10 SDK.

```bash
cd samples/Sample_ExcelConnector
dotnet run
```

The pipeline reads `Data/customers.xlsx` and writes `Data/processed_customers.xlsx`. The input has invalid rows on
purpose, so the run logs validation warnings such as:

```text
Validation failed for Customer ID 0: Invalid ID: 0
Validation failed for Customer ID 13: Invalid age: 200 (must be between 0 and 150)
```

The program then prints the output file's path, size and creation time.

## How it works

### Reading

```csharp
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
```

The source reads the first sheet. The header row binds to `Customer`'s members once per file. Header names match member
names case-insensitively. Columns the class does not have are ignored, and the computed members (`FullName`, `IsAdult`
and the others) have no setter, so the source does not bind them.

Cells convert strictly and culture-invariantly:

- A number cell converts to a numeric member only when no information is lost. `3` converts to `int`, but `1.7` is an
  error.
- A date cell converts to `DateTime`, in UTC.
- A boolean cell converts to `bool`.
- A text cell is parsed, so `"42"` converts to `int`.
- An empty cell is an error for a non-nullable value type such as `int`.

A cell that does not convert is a row error. By default a row error fails the run. This sample's handler logs the row
and skips it. The handler can also return `RowErrorAction.Fail` or `RowErrorAction.DeadLetter`. `RowErrorAction` is in
`NPipeline.Connectors.Errors`. Every cell in the sample data converts, so the handler does not run.

Options are immutable records. Change them with a `with` expression, for example
`o => o with { SheetName = "Customers", SkipRows = 2 }` to read a named sheet with two title rows above the header.

Local files need no storage configuration. The node resolves the file system provider from the URI.

To map rows by hand instead, pass a mapper. `ExcelRow.Get<T>` converts strictly and names the column when it fails:

```csharp
var sourceNode = ExcelConnector.Source(
    StorageUri.FromFilePath(sourcePath),
    row => new Customer
    {
        Id = row.Get<int>("Id"),
        FirstName = row.Get<string>("FirstName"),
        RegistrationDate = row.Get<DateTime>("RegistrationDate"),
    });
```

Use `row.TryGet<T>("name", out var value)` for a column that may be missing or empty.

### Writing

```csharp
var sinkNode = ExcelConnector.Sink<Customer>(
    StorageUri.FromFilePath(targetPath),
    options => options with { FreezeHeader = true, AutoFilter = true });
```

The sink writes a bold header of member names to a sheet named `Sheet1`, then one row per customer. `FreezeHeader`
keeps the header visible while scrolling, and `AutoFilter` adds filter buttons to it.

Columns follow the members' declaration order. `Customer` has no attributes, so the computed members are written too:
the output has the 11 input columns plus `FullName`, `IsAdult`, `AgeCategory`, `NormalizedCountry` and
`CustomerStatus`. To leave a member out, mark it with `[IgnoreColumn]` from `NPipeline.Connectors.Attributes`.

Cells are typed. `int`, `long`, `decimal` and `double` values are number cells, `bool` values are boolean cells, and
`DateTime` values are date cells formatted `yyyy-mm-dd hh:mm:ss`.

The workbook is streamed, so memory stays constant however many rows there are. On the file system, the sink writes to
a temporary file and moves it into place when it finishes.

### Transforms

`ValidationTransform` checks each record:

- `Id` is greater than 0.
- `FirstName`, `LastName`, `Email` and `Country` are not empty, and `Email` looks like an email address.
- `Age` is between 0 and 150.
- `RegistrationDate` is set and not in the future.
- `AccountBalance` and `LoyaltyPoints` are not negative.
- `DiscountPercentage` is between 0 and 100.

It logs a warning for each invalid record and passes the record on. It is created with the default
`filterInvalidRecords = false`. With `filterInvalidRecords = true`, an invalid record throws an
`InvalidOperationException` and fails the run.

`DataTransform` returns a normalized copy of each record:

- Trims names, makes them lowercase, and capitalizes the first letter.
- Trims email addresses and makes them lowercase.
- Expands `USA` and `UK` to `United States` and `United Kingdom`, and title-cases other country names.
- Clamps `DiscountPercentage` to between 0 and 100.

## Sample data

`Data/customers.xlsx` has 17 customer records with 11 columns: `Id`, `FirstName`, `LastName`, `Email`, `Age`,
`RegistrationDate` (a date cell), `Country`, `AccountBalance`, `IsPremiumMember` (a boolean cell),
`DiscountPercentage` and `LoyaltyPoints`.

Records 1 to 10 are valid. Six of the rest each break one validation rule: an `Id` of 0, a missing first name, an
invalid email, an age of 200, a negative balance and a discount of 150. They are logged and still written, so the output
has 17 rows.

## Project structure

```text
Sample_ExcelConnector/
├── Data/
│   ├── customers.xlsx               # Input
│   └── processed_customers.xlsx     # Output, overwritten on each run
├── Nodes/
│   ├── DataTransform.cs             # Normalizes names, email, country and discount
│   └── ValidationTransform.cs       # Validates each record and logs failures
├── Customer.cs                      # Record type with computed members
├── ExcelConnectorPipeline.cs        # Pipeline definition
├── Program.cs                       # Host setup and pipeline run
└── Sample_ExcelConnector.csproj
```

## Learn more

- [Excel Connector](../../docs/connectors/excel.md)
- [File Connectors: Shared Behaviour](../../docs/connectors/file-connectors.md)
