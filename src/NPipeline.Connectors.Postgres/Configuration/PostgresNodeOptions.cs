using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Postgres.Connection;
using NPipeline.Connectors.Postgres.Reliability;
using NPipeline.Connectors.Sql;
using NResilience;

namespace NPipeline.Connectors.Postgres.Configuration;

/// <summary>Options for a PostgreSQL source. Create them with <see cref="PostgresConnector" />, adjusting defaults with <c>with</c>.</summary>
public sealed record PostgresReadOptions : SqlSourceOptions
{
    /// <summary>Creates options whose members map to snake_case columns, PostgreSQL's convention.</summary>
    public PostgresReadOptions() => Naming = ColumnNamingPolicy.SnakeCaseLower;

    /// <summary>A pool of named data sources, used instead of <see cref="SqlNodeOptions.ConnectionString" />.</summary>
    public IPostgresConnectionPool? ConnectionPool { get; init; }

    /// <summary>The pool's connection to use; <c>null</c> uses its default.</summary>
    public string? ConnectionName { get; init; }

    /// <summary>
    ///     Retries connecting and running the query after a transient failure. Nothing has been read at that point, so a
    ///     retry cannot emit a row twice; once rows flow, a failure is not retried. Defaults to
    ///     <see cref="PostgresConnectorResilience.Default" />.
    /// </summary>
    public Resilience Resilience { get; init; } = PostgresConnectorResilience.Default;

    /// <inheritdoc />
    protected override bool HasConnectorConnection => ConnectionPool is not null;
}

/// <summary>Options for a PostgreSQL sink. Create them with <see cref="PostgresConnector" />, adjusting defaults with <c>with</c>.</summary>
public sealed record PostgresWriteOptions : SqlSinkOptions
{
    /// <summary>Creates options whose members map to snake_case columns, PostgreSQL's convention.</summary>
    public PostgresWriteOptions() => Naming = ColumnNamingPolicy.SnakeCaseLower;

    /// <summary>A pool of named data sources, used instead of <see cref="SqlNodeOptions.ConnectionString" />.</summary>
    public IPostgresConnectionPool? ConnectionPool { get; init; }

    /// <summary>The pool's connection to use; <c>null</c> uses its default.</summary>
    public string? ConnectionName { get; init; }

    /// <summary>
    ///     How rows are written: multi-row statements (<see cref="PostgresWriteStrategy.Batch" />, the default), one
    ///     statement per row, or binary <c>COPY</c>, the fastest. <c>COPY</c> does not upsert.
    /// </summary>
    public PostgresWriteStrategy WriteStrategy { get; init; } = PostgresWriteStrategy.Batch;

    /// <summary>
    ///     Retries a batch after a transient failure, with <see cref="SqlTransactionMode.PerBatch" /> (the default), whose
    ///     transaction makes a retry safe. Defaults to <see cref="PostgresConnectorResilience.Default" />.
    /// </summary>
    public Resilience Resilience { get; init; } = PostgresConnectorResilience.Default;

    /// <inheritdoc />
    protected override bool HasConnectorConnection => ConnectionPool is not null;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentNullException.ThrowIfNull(Resilience, nameof(Resilience));

        if (WriteStrategy == PostgresWriteStrategy.Copy && Upsert is not null)
            throw new NotSupportedException("COPY only inserts; use PostgresWriteStrategy.Batch to upsert.");
    }
}
