namespace NPipeline.Connectors.Errors;

/// <summary>
///     A record that a connector could not turn into an item, passed to a <see cref="RowErrorHandler" />. It is a snapshot:
///     unlike the reader's current row, it stays valid after the connector moves on.
/// </summary>
/// <param name="Source">Where the record came from: a file URI without its query string, or a request URI.</param>
/// <param name="RecordNumber">The record's 1-based position in <paramref name="Source" /> (a data row, not counting a header).</param>
/// <param name="Field">The field that failed, when the failure belongs to one field.</param>
/// <param name="RawExcerpt">The start of the raw record or value, when the format has one; <c>null</c> when omitted.</param>
/// <param name="Exception">Why the record failed.</param>
public sealed record RowError(string Source, long RecordNumber, string? Field, string? RawExcerpt, Exception Exception);

/// <summary>What a connector does with a record that failed.</summary>
public enum RowErrorAction
{
    /// <summary>Fail the read with a <see cref="RecordMappingException" />.</summary>
    Fail,

    /// <summary>Drop the record and continue.</summary>
    Skip,

    /// <summary>
    ///     Send a <see cref="ConnectorRecordFailure" /> to the pipeline's dead-letter sink and continue. The read fails
    ///     with <c>DeadLetterSinkNotConfiguredException</c> when the pipeline has no dead-letter sink.
    /// </summary>
    DeadLetter,
}

/// <summary>Decides what happens to a record that a connector could not turn into an item.</summary>
/// <param name="error">The failed record.</param>
/// <returns>The action to take. Without a handler, connectors fail.</returns>
public delegate RowErrorAction RowErrorHandler(RowError error);

/// <summary>
///     The dead-letter item for a record that failed before it became an item. It carries the same fields as
///     <see cref="RowError" />, without the exception, which the dead-letter envelope holds.
/// </summary>
/// <param name="Source">Where the record came from.</param>
/// <param name="RecordNumber">The record's 1-based position in <paramref name="Source" />.</param>
/// <param name="Field">The field that failed, if any.</param>
/// <param name="RawExcerpt">The start of the raw record or value, if the format has one.</param>
public sealed record ConnectorRecordFailure(string Source, long RecordNumber, string? Field, string? RawExcerpt);
