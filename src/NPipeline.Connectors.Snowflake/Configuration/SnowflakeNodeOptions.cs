using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Snowflake.Connection;
using NPipeline.Connectors.Snowflake.Reliability;
using NPipeline.Connectors.Sql;
using NResilience;

namespace NPipeline.Connectors.Snowflake.Configuration;

/// <summary>Options for a Snowflake source. Create them with <see cref="SnowflakeConnector" />, adjusting defaults with <c>with</c>.</summary>
public sealed record SnowflakeReadOptions : SqlSourceOptions
{
    /// <summary>Creates options whose members map to UPPER_SNAKE columns, as Snowflake stores unquoted names.</summary>
    public SnowflakeReadOptions() => Naming = ColumnNamingPolicy.SnakeCaseUpper;

    /// <summary>A pool of named connection strings, used instead of <see cref="SqlNodeOptions.ConnectionString" />.</summary>
    public ISnowflakeConnectionPool? ConnectionPool { get; init; }

    /// <summary>The pool's connection to use; <c>null</c> uses its default.</summary>
    public string? ConnectionName { get; init; }

    /// <summary>
    ///     Retries connecting and running the query after a transient failure. Nothing has been read at that point, so a
    ///     retry cannot emit a row twice; once rows flow, a failure is not retried. Defaults to
    ///     <see cref="SnowflakeConnectorResilience.Default" />.
    /// </summary>
    public Resilience Resilience { get; init; } = SnowflakeConnectorResilience.Default;

    /// <inheritdoc />
    protected override bool HasConnectorConnection => ConnectionPool is not null;
}

/// <summary>Options for a Snowflake sink. Create them with <see cref="SnowflakeConnector" />, adjusting defaults with <c>with</c>.</summary>
public sealed record SnowflakeWriteOptions : SqlSinkOptions
{
    /// <summary>Creates options whose members map to UPPER_SNAKE columns, as Snowflake stores unquoted names.</summary>
    public SnowflakeWriteOptions() => Naming = ColumnNamingPolicy.SnakeCaseUpper;

    /// <summary>A pool of named connection strings, used instead of <see cref="SqlNodeOptions.ConnectionString" />.</summary>
    public ISnowflakeConnectionPool? ConnectionPool { get; init; }

    /// <summary>The pool's connection to use; <c>null</c> uses its default.</summary>
    public string? ConnectionName { get; init; }

    /// <summary>
    ///     How rows are written: multi-row statements (<see cref="SnowflakeWriteStrategy.Batch" />, the default), one
    ///     statement per row, or a CSV file staged with <c>PUT</c> and loaded with <c>COPY INTO</c>, the fastest for large
    ///     loads. Staged copies do not upsert.
    /// </summary>
    public SnowflakeWriteStrategy WriteStrategy { get; init; } = SnowflakeWriteStrategy.Batch;

    /// <summary>The internal stage staged copies upload to: <c>~</c> (the user stage, the default) or a named stage.</summary>
    public string Stage { get; init; } = "~";

    /// <summary>The prefix of staged file names.</summary>
    public string StageFilePrefix { get; init; } = "npipeline_";

    /// <summary>Whether <c>COPY INTO</c> removes a staged file once loaded. Defaults to <c>true</c>.</summary>
    public bool PurgeStagedFiles { get; init; } = true;

    /// <summary>
    ///     Retries a batch after a transient failure, with <see cref="SqlTransactionMode.PerBatch" /> (the default), whose
    ///     transaction makes a retry safe. Defaults to <see cref="SnowflakeConnectorResilience.Default" />.
    /// </summary>
    public Resilience Resilience { get; init; } = SnowflakeConnectorResilience.Default;

    /// <inheritdoc />
    protected override bool HasConnectorConnection => ConnectionPool is not null;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentException.ThrowIfNullOrWhiteSpace(Stage, nameof(Stage));
        ArgumentNullException.ThrowIfNull(StageFilePrefix, nameof(StageFilePrefix));
        ArgumentNullException.ThrowIfNull(Resilience, nameof(Resilience));

        if (WriteStrategy == SnowflakeWriteStrategy.StagedCopy && Upsert is not null)
            throw new NotSupportedException("A staged copy only inserts; use SnowflakeWriteStrategy.Batch to upsert.");
    }
}
