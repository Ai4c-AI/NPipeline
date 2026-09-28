# Sample 12: JSON Connector

This sample reads customer records from a JSON file, validates and normalizes them, and writes the result to another
JSON file with the `NPipeline.Connectors.Json` connector.

## What it demonstrates

- Creating a JSON source and sink with `JsonConnector.Source<T>` and `JsonConnector.Sink<T>`.
- Mapping properties with `[Column]` and `[IgnoreColumn]`.
- Writing indented output.
- Reading NDJSON, in an optional second pipeline branch.
- Validation and enrichment in custom transform nodes.

The pipeline is:

```text
JsonSourceNode<Customer> -> ValidationTransform -> DataTransform -> JsonSinkNode<Customer>
```

## Running the sample

Requires the .NET 10 SDK.

```bash
cd samples/Sample_JsonConnector
dotnet run
```

The pipeline reads `Data/customers.json` and writes `Data/processed_customers.json`. The program then prints the output
file:

```text
Output file content:
[
  {
    "id": 1,
    "firstName": "John",
    "lastName": "Doe",
    "email": "john.doe@example.com",
    "age": 28,
    "registrationDate": "2023-01-15T00:00:00Z",
    "country": "United States",
    "isActive": true
  },
  ...
]
```

## How it works

### Reading

```csharp
var sourceNode = JsonConnector.Source<Customer>(StorageUri.FromFilePath(sourcePath));
```

Each record is deserialized with System.Text.Json. By default the connector uses System.Text.Json's web defaults, with
enums as names:

- Property names are camelCase when writing and match case-insensitively when reading.
- Numbers can also be read from strings, such as `"12"`.
- Dates are ISO 8601.

The default format, `JsonFormat.Auto`, looks at each file's first character. `[` means an array, and anything else
means a sequence of top-level values (NDJSON). To read an array nested inside a root object, set `ItemsPath`:

```csharp
// {"data": {"items": [ ... ]}}
var sourceNode = JsonConnector.Source<Customer>(uri, o => o with { ItemsPath = "data.items" });
```

A record that does not deserialize fails the run by default. To skip it instead, set a `RowErrorHandler`. The handler
can return `RowErrorAction.Fail`, `Skip` or `DeadLetter` (in `NPipeline.Connectors.Errors`):

```csharp
var sourceNode = JsonConnector.Source<Customer>(uri, o => o with { RowErrorHandler = _ => RowErrorAction.Skip });
```

Options are immutable records. Change them with a `with` expression.

### Mapping

`Customer` uses the shared attributes from `NPipeline.Connectors.Attributes`:

```csharp
public class Customer
{
    [Column("id")]
    public int Id { get; set; }

    [Column("firstName")]
    public string FirstName { get; set; } = string.Empty;

    // LastName, Email, Age, RegistrationDate, Country and IsActive follow the same pattern.

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

`[Column("name")]` sets the JSON property name. `[IgnoreColumn]` leaves a member out. `[JsonPropertyName]` wins over
`[Column]`. In this sample the `[Column]` names match the camelCase defaults, so the attributes only make the mapping
explicit. Use them when the file's property names differ, for example `[Column("cust_id")]`.

For other naming rules or converters, pass your own `JsonSerializerOptions`:

```csharp
var serializerOptions = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };
var sourceNode = JsonConnector.Source<Customer>(uri, o => o with { SerializerOptions = serializerOptions });
```

### Writing

```csharp
var sinkNode = JsonConnector.Sink<Customer>(
    StorageUri.FromFilePath(targetPath),
    options => options with { WriteIndented = true });
```

The sink writes a JSON array. When the file name ends in `.ndjson` or `.jsonl`, it writes NDJSON instead, one record per
line. `WriteIndented` applies to arrays only. To choose the format explicitly, set `Format` to `JsonFormat.Array` or
`JsonFormat.NewlineDelimited`.

### NDJSON

`JsonConnectorPipeline.cs` contains a second branch, commented out, that reads `Data/customers.ndjson` and writes
`Data/processed_customers.ndjson`. Uncomment it to run both branches. The NDJSON branch needs no extra options: the
source detects NDJSON from the content, and the sink picks NDJSON from the file name.

Records in NDJSON may span lines. `Data/customers.ndjson` holds pretty-printed objects one after another, and each is
read as one record.

### Transforms

`ValidationTransform` checks each record:

- `Id` is greater than 0.
- `FirstName`, `LastName` and `Country` are not empty.
- `Email` is present and looks like an email address.
- `Age` is between 0 and 150.
- `RegistrationDate` is set and not in the future.

It logs a warning for each failure. In this sample it is created with the default `filterInvalidRecords = false`, so
invalid records are logged and passed on. With `filterInvalidRecords = true`, an invalid record throws an
`InvalidOperationException`.

`DataTransform` returns a normalized copy of each record:

- Trims names and capitalizes the first letter.
- Trims email addresses and makes them lowercase.
- Expands `USA` and `UK` to `United States` and `United Kingdom`, and title-cases other country names.

## Sample data

`Data/customers.json` has 13 customer records. Records 11 to 13 break validation rules (an `id` of 0, a negative age,
a missing first name, a missing email). Because validation only logs, all 13 records reach the output.

## Project structure

```text
Sample_JsonConnector/
├── Data/
│   ├── customers.json              # Input, JSON array
│   ├── customers.ndjson            # Input for the optional NDJSON branch
│   └── processed_customers.json    # Output, overwritten on each run
├── Nodes/
│   ├── DataTransform.cs            # Normalizes names, email and country
│   └── ValidationTransform.cs      # Validates each record
├── Customer.cs                     # Record type with [Column] and [IgnoreColumn]
├── JsonConnectorPipeline.cs        # Pipeline definition
├── Program.cs                      # Host setup and pipeline run
└── Sample_JsonConnector.csproj
```

## Learn more

- [JSON Connector](../../docs/connectors/json.md)
- [File Connectors: Shared Behaviour](../../docs/connectors/file-connectors.md)
