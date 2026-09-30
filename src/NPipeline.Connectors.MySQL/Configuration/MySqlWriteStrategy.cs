namespace NPipeline.Connectors.MySql.Configuration;

/// <summary>
///     Write strategies supported by the MySQL sink.
/// </summary>
public enum MySqlWriteStrategy
{
    /// <summary>
    ///     Write each row individually using separate INSERT statements.
    /// </summary>
    PerRow,

    /// <summary>
    ///     Write rows using batched multi-row INSERT commands. This is the default strategy.
    /// </summary>
    Batch,

    /// <summary>
    ///     Write rows using <c>MySqlBulkLoader</c> via the <c>LOAD DATA LOCAL INFILE</c> protocol
    ///     for highest throughput (10–50× faster than batch INSERT).
    ///     Requires both client (<c>AllowLoadLocalInfile=true</c> in the connection string) and
    ///     server <c>local_infile</c> to be enabled.
    /// </summary>
    BulkLoad,
}
