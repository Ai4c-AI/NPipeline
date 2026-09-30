using NPipeline.Connectors.Diagnostics;
using NPipeline.ErrorHandling;

namespace NPipeline.Connectors.Errors;

/// <summary>Applies a source's <see cref="RowErrorHandler" /> to a row that failed: fail, skip or dead-letter, and count it.</summary>
internal static class RowErrorDispatch
{
    public static async ValueTask HandleAsync(
        RowErrorHandler? handler,
        int excerptLength,
        DeadLetterChannel deadLetters,
        string connector,
        string scheme,
        string source,
        long recordNumber,
        Exception exception,
        string? rawRecord,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(exception);

        var field = (exception as FieldMappingException)?.Column;
        var error = new RowError(source, recordNumber, field, Excerpt(rawRecord, excerptLength), exception);
        var action = handler?.Invoke(error) ?? RowErrorAction.Fail;

        ConnectorDiagnostics.RecordRowError(connector, scheme, action.ToString().ToLowerInvariant());

        switch (action)
        {
            case RowErrorAction.Skip:
                return;
            case RowErrorAction.DeadLetter:
                var failure = new ConnectorRecordFailure(error.Source, error.RecordNumber, error.Field, error.RawExcerpt);
                await deadLetters.SendAsync(failure, exception, cancellationToken).ConfigureAwait(false);
                return;
            default:
                throw new RecordMappingException(error);
        }
    }

    private static string? Excerpt(string? raw, int length)
    {
        if (raw is null || length == 0)
            return null;

        return raw.Length <= length
            ? raw
            : string.Concat(raw.AsSpan(0, length), "…");
    }
}
