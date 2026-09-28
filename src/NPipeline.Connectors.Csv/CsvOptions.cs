using System.Globalization;
using System.Text;
using CsvHelper.Configuration;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Csv;

/// <summary>Options for <see cref="CsvSourceNode{T}" />. Create them with <see cref="CsvConnector.Source{T}(NPipeline.StorageProviders.Models.StorageUri, Func{CsvReadOptions, CsvReadOptions}?)" /> or directly.</summary>
public sealed record CsvReadOptions : FileSourceOptions
{
    /// <summary>The field delimiter. Defaults to <c>,</c>.</summary>
    public string Delimiter { get; init; } = ",";

    /// <summary>Whether to detect the delimiter from the start of each file instead of using <see cref="Delimiter" />. Defaults to <c>false</c>.</summary>
    public bool DetectDelimiter { get; init; }

    /// <summary>The quote character. Defaults to <c>"</c>.</summary>
    public char Quote { get; init; } = '"';

    /// <summary>
    ///     Whether the first row is a header. When <c>null</c> (the default), a header is expected, except when a scalar
    ///     type (<c>string</c>, <c>int</c>, <c>DateTime</c>…) is read without a manual mapper. Without a header, columns
    ///     map to members in declaration order, the order <see cref="CsvSinkNode{T}" /> writes them.
    /// </summary>
    public bool? HasHeader { get; init; }

    /// <summary>The culture for numbers. Defaults to the invariant culture. Dates are read as ISO 8601 or in this culture's format; values without an offset are UTC.</summary>
    public CultureInfo Culture { get; init; } = CultureInfo.InvariantCulture;

    /// <summary>Whether to trim whitespace around fields. Defaults to <see cref="TrimOptions.None" />.</summary>
    public TrimOptions Trim { get; init; } = TrimOptions.None;

    /// <summary>The text encoding. When <c>null</c> (the default), UTF-8, or the encoding a byte order mark names.</summary>
    public Encoding? Encoding { get; init; }

    /// <summary>How member names become column names. Defaults to <see cref="ColumnNamingPolicy.AsIs" />; columns match case-insensitively.</summary>
    public ColumnNamingPolicy Naming { get; init; } = ColumnNamingPolicy.AsIs;

    /// <summary>What to do when a mapped member has no column. Defaults to <see cref="MissingColumnBehavior.ThrowForRequired" />.</summary>
    public MissingColumnBehavior MissingColumns { get; init; } = MissingColumnBehavior.ThrowForRequired;

    /// <summary>Adjusts CsvHelper's parser configuration after the connector applies these options. An escape hatch for settings not exposed here.</summary>
    public Action<CsvConfiguration>? ConfigureCsvHelper { get; init; }

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentException.ThrowIfNullOrEmpty(Delimiter, nameof(Delimiter));
        ArgumentNullException.ThrowIfNull(Culture, nameof(Culture));
        ArgumentNullException.ThrowIfNull(Naming, nameof(Naming));
    }
}

/// <summary>Options for <see cref="CsvSinkNode{T}" />. Create them with <see cref="CsvConnector.Sink{T}(NPipeline.StorageProviders.Models.StorageUri, Func{CsvWriteOptions, CsvWriteOptions}?)" /> or directly.</summary>
public sealed record CsvWriteOptions : FileSinkOptions
{
    /// <summary>The field delimiter. Defaults to <c>,</c>.</summary>
    public string Delimiter { get; init; } = ",";

    /// <summary>The quote character. Defaults to <c>"</c>.</summary>
    public char Quote { get; init; } = '"';

    /// <summary>Whether to write a header row. When <c>null</c> (the default), records get a header and scalar types do not.</summary>
    public bool? HasHeader { get; init; }

    /// <summary>The culture for numbers. Defaults to the invariant culture. Dates are always written as ISO 8601.</summary>
    public CultureInfo Culture { get; init; } = CultureInfo.InvariantCulture;

    /// <summary>The text encoding. When <c>null</c> (the default), UTF-8 without a byte order mark.</summary>
    public Encoding? Encoding { get; init; }

    /// <summary>The line terminator. Defaults to <c>\n</c>.</summary>
    public string NewLine { get; init; } = "\n";

    /// <summary>How member names become column names. Defaults to <see cref="ColumnNamingPolicy.AsIs" />.</summary>
    public ColumnNamingPolicy Naming { get; init; } = ColumnNamingPolicy.AsIs;

    /// <summary>Adjusts CsvHelper's writer configuration after the connector applies these options. An escape hatch for settings not exposed here.</summary>
    public Action<CsvConfiguration>? ConfigureCsvHelper { get; init; }

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentException.ThrowIfNullOrEmpty(Delimiter, nameof(Delimiter));
        ArgumentException.ThrowIfNullOrEmpty(NewLine, nameof(NewLine));
        ArgumentNullException.ThrowIfNull(Culture, nameof(Culture));
        ArgumentNullException.ThrowIfNull(Naming, nameof(Naming));
    }
}
