namespace NPipeline.Connectors.Errors;

/// <summary>A record could not be turned into an item, and its <see cref="RowErrorHandler" /> chose to fail (or there was none).</summary>
public sealed class RecordMappingException : Exception
{
    /// <summary>Creates the exception for <paramref name="error" />, with its exception as the inner exception.</summary>
    /// <param name="error">The failed record.</param>
    public RecordMappingException(RowError error)
        : base(BuildMessage(error), error?.Exception)
    {
        ArgumentNullException.ThrowIfNull(error);
        RecordSource = error.Source;
        RecordNumber = error.RecordNumber;
        Field = error.Field;
        RawExcerpt = error.RawExcerpt;
    }

    /// <summary>Where the record came from (named so as not to hide <see cref="Exception.Source" />).</summary>
    public string RecordSource { get; }

    /// <summary>The record's 1-based position in <see cref="RecordSource" />.</summary>
    public long RecordNumber { get; }

    /// <summary>The field that failed, if any.</summary>
    public string? Field { get; }

    /// <summary>The start of the raw record or value, if the format has one.</summary>
    public string? RawExcerpt { get; }

    private static string BuildMessage(RowError error)
    {
        ArgumentNullException.ThrowIfNull(error);

        var field = error.Field is null
            ? string.Empty
            : $", field '{error.Field}'";

        return $"Record {error.RecordNumber} of '{error.Source}'{field} could not be mapped: {error.Exception.Message}";
    }
}

/// <summary>One field of a record could not be read into its member. Record mappers throw it; file sources turn it into a <see cref="RowError" />.</summary>
public sealed class FieldMappingException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="member">The CLR member being set.</param>
    /// <param name="column">The column read for it.</param>
    /// <param name="innerException">Why the value could not be read.</param>
    public FieldMappingException(string member, string column, Exception innerException)
        : base($"Column '{column}' could not be mapped to '{member}': {innerException?.Message}", innerException)
    {
        Member = member;
        Column = column;
    }

    /// <summary>The CLR member being set.</summary>
    public string Member { get; }

    /// <summary>The column read for it.</summary>
    public string Column { get; }
}

/// <summary>A raw value could not be converted to its target type. Conversions throw it rather than returning a default.</summary>
public sealed class FieldConversionException : FormatException
{
    private const int MaxRawLength = 64;

    /// <summary>Creates the exception.</summary>
    /// <param name="targetType">The type the value was being converted to.</param>
    /// <param name="rawValue">The raw value, or <c>null</c> for a missing value. It is truncated to 64 characters.</param>
    /// <param name="reason">Why the conversion failed, if more specific than "cannot convert".</param>
    /// <param name="innerException">The underlying exception, if any.</param>
    public FieldConversionException(Type targetType, string? rawValue, string? reason = null, Exception? innerException = null)
        : base(BuildMessage(targetType, rawValue, reason), innerException)
    {
        TargetType = targetType;
        RawValue = Truncate(rawValue);
    }

    /// <summary>The type the value was being converted to.</summary>
    public Type TargetType { get; }

    /// <summary>The start of the raw value (at most 64 characters), or <c>null</c> for a missing value.</summary>
    public string? RawValue { get; }

    private static string? Truncate(string? value) =>
        value is { Length: > MaxRawLength }
            ? string.Concat(value.AsSpan(0, MaxRawLength), "…")
            : value;

    private static string BuildMessage(Type targetType, string? rawValue, string? reason)
    {
        ArgumentNullException.ThrowIfNull(targetType);

        var value = rawValue is null
            ? "a missing value"
            : $"'{Truncate(rawValue)}'";

        return reason is null
            ? $"Cannot convert {value} to {targetType.Name}."
            : $"Cannot convert {value} to {targetType.Name}: {reason}.";
    }
}

/// <summary>
///     A record type could not be bound to a file's columns: required columns are missing, or the type cannot be
///     constructed. Thrown once, when the columns are known, rather than once per row.
/// </summary>
public sealed class RecordBindingException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="recordType">The record type being bound.</param>
    /// <param name="reason">Why binding failed, when it is not missing columns (the type cannot be constructed).</param>
    /// <param name="missingColumns">The columns the record needs but the file does not have.</param>
    /// <param name="availableColumns">The columns the file has.</param>
    public RecordBindingException(Type recordType, string? reason, IReadOnlyList<string> missingColumns, IReadOnlyList<string> availableColumns)
        : base(BuildMessage(recordType, reason, missingColumns, availableColumns))
    {
        RecordType = recordType;
        MissingColumns = missingColumns;
        AvailableColumns = availableColumns;
    }

    /// <summary>The record type being bound.</summary>
    public Type RecordType { get; }

    /// <summary>The columns the record needs but the file does not have.</summary>
    public IReadOnlyList<string> MissingColumns { get; }

    /// <summary>The columns the file has.</summary>
    public IReadOnlyList<string> AvailableColumns { get; }

    private static string BuildMessage(Type recordType, string? reason, IReadOnlyList<string> missingColumns, IReadOnlyList<string> availableColumns)
    {
        ArgumentNullException.ThrowIfNull(recordType);
        ArgumentNullException.ThrowIfNull(missingColumns);
        ArgumentNullException.ThrowIfNull(availableColumns);

        if (reason is not null)
            return $"Cannot map records to {recordType.Name}: {reason}";

        return $"Cannot map records to {recordType.Name}: missing column(s) {string.Join(", ", missingColumns.Select(c => $"'{c}'"))}. " +
               $"Available columns: {(availableColumns.Count == 0 ? "(none)" : string.Join(", ", availableColumns.Select(c => $"'{c}'")))}.";
    }
}
