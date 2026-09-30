using NPipeline.Connectors.MySql.Connection;
using NPipeline.Connectors.MySql.Reliability;
using NPipeline.Connectors.Sql;
using NResilience;

namespace NPipeline.Connectors.MySql.Configuration;

/// <summary>Options for a MySQL source. Create them with <see cref="MySqlNodes" />, adjusting defaults with <c>with</c>.</summary>
public sealed record MySqlReadOptions : SqlSourceOptions
{
    /// <summary>A pool of named connection strings, used instead of <see cref="SqlNodeOptions.ConnectionString" />.</summary>
    public IMySqlConnectionPool? ConnectionPool { get; init; }

    /// <summary>The pool's connection to use; <c>null</c> uses its default.</summary>
    public string? ConnectionName { get; init; }

    /// <summary>
    ///     Retries connecting and running the query after a transient failure. Nothing has been read at that point, so a
    ///     retry cannot emit a row twice; once rows flow, a failure is not retried. Defaults to
    ///     <see cref="MySqlConnectorResilience.Default" />.
    /// </summary>
    public Resilience Resilience { get; init; } = MySqlConnectorResilience.Default;

    /// <inheritdoc />
    protected override bool HasConnectorConnection => ConnectionPool is not null;
}

/// <summary>Options for a MySQL sink. Create them with <see cref="MySqlNodes" />, adjusting defaults with <c>with</c>.</summary>
public sealed record MySqlWriteOptions : SqlSinkOptions
{
    /// <summary>A pool of named connection strings, used instead of <see cref="SqlNodeOptions.ConnectionString" />.</summary>
    public IMySqlConnectionPool? ConnectionPool { get; init; }

    /// <summary>The pool's connection to use; <c>null</c> uses its default.</summary>
    public string? ConnectionName { get; init; }

    /// <summary>
    ///     How rows are written: multi-row statements (<see cref="MySqlWriteStrategy.Batch" />, the default), one statement
    ///     per row, or <c>LOAD DATA LOCAL INFILE</c> through <c>MySqlBulkCopy</c>, which needs
    ///     <c>AllowLoadLocalInfile=true</c> in the connection string and <c>local_infile</c> on the server. Bulk loads do
    ///     not upsert.
    /// </summary>
    public MySqlWriteStrategy WriteStrategy { get; init; } = MySqlWriteStrategy.Batch;

    /// <summary>
    ///     Retries a batch after a transient failure, with <see cref="SqlTransactionMode.PerBatch" /> (the default), whose
    ///     transaction makes a retry safe. Defaults to <see cref="MySqlConnectorResilience.Default" />.
    /// </summary>
    public Resilience Resilience { get; init; } = MySqlConnectorResilience.Default;

    /// <inheritdoc />
    protected override bool HasConnectorConnection => ConnectionPool is not null;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentNullException.ThrowIfNull(Resilience, nameof(Resilience));

        if (WriteStrategy == MySqlWriteStrategy.BulkLoad && Upsert is not null)
            throw new NotSupportedException("A bulk load only inserts; use MySqlWriteStrategy.Batch to upsert.");
    }
}
