using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.DuckDB.Configuration;

/// <summary>Options for a DuckDB source. Create them with <see cref="DuckDBConnector" />, adjusting defaults with <c>with</c>.</summary>
public sealed record DuckDBReadOptions : SqlSourceOptions
{
    /// <summary>The database. Defaults to an in-memory database, which suits queries over files (<c>read_parquet</c> and friends).</summary>
    public DuckDBDatabase Database { get; init; } = DuckDBDatabase.InMemory;

    /// <inheritdoc />
    protected override bool HasConnectorConnection => ConnectionString is null && Uri is null;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentNullException.ThrowIfNull(Database, nameof(Database));
        Database.Validate();
    }
}

/// <summary>Options for a DuckDB sink. Create them with <see cref="DuckDBConnector" />, adjusting defaults with <c>with</c>.</summary>
public sealed record DuckDBWriteOptions : SqlSinkOptions
{
    /// <summary>The database. Defaults to an in-memory database, which suits <see cref="ExportTo" />.</summary>
    public DuckDBDatabase Database { get; init; } = DuckDBDatabase.InMemory;

    /// <summary>
    ///     How rows are written: DuckDB's appender (<see cref="DuckDBWriteStrategy.Appender" />, the default and the
    ///     fastest), or multi-row <c>INSERT</c> statements, which can also upsert.
    /// </summary>
    public DuckDBWriteStrategy WriteStrategy { get; init; } = DuckDBWriteStrategy.Appender;

    /// <summary>Creates the table from the record's members when it does not exist. Defaults to <c>true</c>.</summary>
    public bool AutoCreateTable { get; init; } = true;

    /// <summary>Deletes the table's rows before writing. Defaults to <c>false</c>.</summary>
    public bool TruncateBeforeWrite { get; init; }

    /// <summary>After writing, exports the table to this file (Parquet, CSV or JSON, by its extension or <see cref="Export" />).</summary>
    public string? ExportTo { get; init; }

    /// <summary>How <see cref="ExportTo" /> is written.</summary>
    public DuckDBFileExportOptions Export { get; init; } = new();

    /// <inheritdoc />
    protected override bool HasConnectorConnection => ConnectionString is null && Uri is null;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentNullException.ThrowIfNull(Database, nameof(Database));
        ArgumentNullException.ThrowIfNull(Export, nameof(Export));
        Database.Validate();

        if (WriteStrategy == DuckDBWriteStrategy.Appender && Upsert is not null)
            throw new NotSupportedException("The appender only inserts; use DuckDBWriteStrategy.Sql to upsert.");
    }
}
