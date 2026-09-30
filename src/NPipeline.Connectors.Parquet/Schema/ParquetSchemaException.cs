namespace NPipeline.Connectors.Parquet;

/// <summary>A file's schema was rejected, by <see cref="ParquetReadOptions.SchemaValidator" /> or because a record type does not fit it.</summary>
public sealed class ParquetSchemaException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="message">What was wrong.</param>
    public ParquetSchemaException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">What was wrong.</param>
    /// <param name="innerException">The underlying failure.</param>
    public ParquetSchemaException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
