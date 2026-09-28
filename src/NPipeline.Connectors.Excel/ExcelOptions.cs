using System.Buffers;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Mapping;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Excel;

/// <summary>Options for <see cref="ExcelSourceNode{T}" />. Create them with <see cref="ExcelConnector.Source{T}(StorageUri, Func{ExcelReadOptions, ExcelReadOptions}?)" /> or directly.</summary>
public sealed record ExcelReadOptions : FileSourceOptions
{
    /// <summary>The sheet to read, matched case-insensitively. When <c>null</c> (the default), the sheet at <see cref="SheetIndex" />.</summary>
    public string? SheetName { get; init; }

    /// <summary>The 0-based position of the sheet to read when <see cref="SheetName" /> is <c>null</c>. Defaults to 0, the first sheet.</summary>
    public int SheetIndex { get; init; }

    /// <summary>
    ///     Whether the first row read is a header. When <c>null</c> (the default), a header is expected, except when a
    ///     scalar type is read without a manual mapper. Without a header, columns map to members in declaration order.
    /// </summary>
    public bool? HasHeader { get; init; }

    /// <summary>Rows to skip at the top of the sheet before the header, such as a title. Defaults to 0.</summary>
    public int SkipRows { get; init; }

    /// <summary>Whether to skip rows whose cells are all empty. Defaults to <c>true</c>, since sheets often end in formatted but empty rows.</summary>
    public bool SkipEmptyRows { get; init; } = true;

    /// <summary>The workbook's password, for encrypted files.</summary>
    public string? Password { get; init; }

    /// <summary>How member names become column names. Defaults to <see cref="ColumnNamingPolicy.AsIs" />; headers match case-insensitively.</summary>
    public ColumnNamingPolicy Naming { get; init; } = ColumnNamingPolicy.AsIs;

    /// <summary>What to do when a mapped member has no column. Defaults to <see cref="MissingColumnBehavior.ThrowForRequired" />.</summary>
    public MissingColumnBehavior MissingColumns { get; init; } = MissingColumnBehavior.ThrowForRequired;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentOutOfRangeException.ThrowIfNegative(SheetIndex, nameof(SheetIndex));
        ArgumentOutOfRangeException.ThrowIfNegative(SkipRows, nameof(SkipRows));
        ArgumentNullException.ThrowIfNull(Naming, nameof(Naming));
    }
}

/// <summary>Options for <see cref="ExcelSinkNode{T}" />. Create them with <see cref="ExcelConnector.Sink{T}(StorageUri, Func{ExcelWriteOptions, ExcelWriteOptions}?)" /> or directly.</summary>
public sealed record ExcelWriteOptions : FileSinkOptions
{
    private static readonly SearchValues<char> InvalidSheetNameCharacters = SearchValues.Create(@"[]:*?/\");

    /// <summary>The sheet's name: 1 to 31 characters, without <c>[ ] : * ? / \</c>. Defaults to <c>Sheet1</c>.</summary>
    public string SheetName { get; init; } = "Sheet1";

    /// <summary>Whether to write a header row. When <c>null</c> (the default), records get a header and scalar types do not.</summary>
    public bool? HasHeader { get; init; }

    /// <summary>Whether the header row is bold. Defaults to <c>true</c>.</summary>
    public bool BoldHeader { get; init; } = true;

    /// <summary>Whether to freeze the header row, so it stays visible while scrolling. Defaults to <c>false</c>.</summary>
    public bool FreezeHeader { get; init; }

    /// <summary>Whether to add filter buttons to the header row. Defaults to <c>false</c>.</summary>
    public bool AutoFilter { get; init; }

    /// <summary>How member names become column names. Defaults to <see cref="ColumnNamingPolicy.AsIs" />.</summary>
    public ColumnNamingPolicy Naming { get; init; } = ColumnNamingPolicy.AsIs;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentNullException.ThrowIfNull(Naming, nameof(Naming));

        if (string.IsNullOrWhiteSpace(SheetName) || SheetName.Length > 31 || SheetName.AsSpan().IndexOfAny(InvalidSheetNameCharacters) >= 0
            || SheetName.StartsWith('\'') || SheetName.EndsWith('\'') || SheetName.Equals("History", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                $"'{SheetName}' is not a valid sheet name: use 1 to 31 characters, without [ ] : * ? / \\ or a leading or trailing apostrophe, and not 'History'.",
                nameof(SheetName));
        }
    }
}
