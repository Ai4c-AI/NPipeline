using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.Connectors.SqlServer.Reliability;
using NPipeline.Connectors.Sql;
using NResilience;

namespace NPipeline.Connectors.SqlServer.Configuration;

/// <summary>Options for a SQL Server source. Create them with <see cref="SqlServerConnector" />, adjusting defaults with <c>with</c>.</summary>
public sealed record SqlServerReadOptions : SqlSourceOptions
{
    /// <summary>A pool of named connection strings, used instead of <see cref="SqlNodeOptions.ConnectionString" />.</summary>
    public ISqlServerConnectionPool? ConnectionPool { get; init; }

    /// <summary>The pool's connection to use; <c>null</c> uses its default.</summary>
    public string? ConnectionName { get; init; }

    /// <summary>
    ///     Retries connecting and running the query after a transient failure. Nothing has been read at that point, so a
    ///     retry cannot emit a row twice; once rows flow, a failure is not retried. Defaults to
    ///     <see cref="SqlServerConnectorResilience.Default" />.
    /// </summary>
    public Resilience Resilience { get; init; } = SqlServerConnectorResilience.Default;

    /// <inheritdoc />
    protected override bool HasConnectorConnection => ConnectionPool is not null;
}

/// <summary>Options for a SQL Server sink. Create them with <see cref="SqlServerConnector" />, adjusting defaults with <c>with</c>.</summary>
public sealed record SqlServerWriteOptions : SqlSinkOptions
{
    /// <summary>A pool of named connection strings, used instead of <see cref="SqlNodeOptions.ConnectionString" />.</summary>
    public ISqlServerConnectionPool? ConnectionPool { get; init; }

    /// <summary>The pool's connection to use; <c>null</c> uses its default.</summary>
    public string? ConnectionName { get; init; }

    /// <summary>
    ///     How rows are written: multi-row statements (<see cref="SqlServerWriteStrategy.Batch" />, the default), one
    ///     statement per row, or <c>SqlBulkCopy</c>. Bulk copy does not upsert.
    /// </summary>
    public SqlServerWriteStrategy WriteStrategy { get; init; } = SqlServerWriteStrategy.Batch;

    /// <summary>The bulk copy timeout in seconds. Defaults to 30; 0 waits indefinitely.</summary>
    public int BulkCopyTimeout { get; init; } = DefaultCommandTimeout;

    /// <summary>
    ///     Retries a batch after a transient failure, with <see cref="SqlTransactionMode.PerBatch" /> (the default), whose
    ///     transaction makes a retry safe. Defaults to <see cref="SqlServerConnectorResilience.Default" />.
    /// </summary>
    public Resilience Resilience { get; init; } = SqlServerConnectorResilience.Default;

    /// <inheritdoc />
    protected override bool HasConnectorConnection => ConnectionPool is not null;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentOutOfRangeException.ThrowIfNegative(BulkCopyTimeout, nameof(BulkCopyTimeout));
        ArgumentNullException.ThrowIfNull(Resilience, nameof(Resilience));

        if (WriteStrategy == SqlServerWriteStrategy.BulkCopy && Upsert is not null)
            throw new NotSupportedException("SqlBulkCopy only inserts; use SqlServerWriteStrategy.Batch to upsert.");
    }
}
