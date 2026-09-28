using System.Globalization;
using System.Reflection;
using System.Text;
using System.Xml;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Excel.Xlsx;

/// <summary>
///     Writes one sheet's XML, row by row. Markup and numbers are written raw; only text goes through the
///     <see cref="XmlWriter" />'s escaping.
/// </summary>
internal sealed class XlsxSheetWriter
{
    /// <summary>The most rows a sheet holds.</summary>
    public const int MaxRows = 1_048_576;

    /// <summary>The most columns a sheet holds.</summary>
    public const int MaxColumns = 16_384;

    /// <summary>The longest text a cell holds.</summary>
    public const int MaxCellText = 32_767;

    // Doubles hold integers exactly up to 2^53; larger ones are written as text so no digit is lost.
    private const long MaxExactInteger = 1L << 53;

    private readonly string[] _columnLetters;
    private readonly char[] _scratch = new char[64];
    private readonly XmlWriter _xml;
    private int _column;
    private string _rowText = "0";

    public XlsxSheetWriter(XmlWriter xml, int columnCount)
    {
        _xml = xml;
        _columnLetters = Enumerable.Range(0, Math.Max(columnCount, 1)).Select(ExcelFieldReader.ColumnLetters).ToArray();
    }

    /// <summary>The number of rows written, including the header.</summary>
    public int Rows { get; private set; }

    public static XmlWriterSettings Settings { get; } = new()
    {
        Encoding = new UTF8Encoding(false),

        // Characters XML cannot hold are escaped before writing (_xHHHH_), and a carriage return becomes &#xD;, so a
        // reader gets back exactly what was written.
        CheckCharacters = false,
        NewLineHandling = NewLineHandling.Entitize,
        CloseOutput = false,
    };

    public void BeginSheet(bool freezeHeader)
    {
        _xml.WriteStartDocument(true);
        _xml.WriteStartElement("worksheet", XlsxPackage.MainNamespace);

        if (freezeHeader)
            _xml.WriteRaw("""<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/><selection pane="bottomLeft"/></sheetView></sheetViews>""");

        _xml.WriteStartElement("sheetData", XlsxPackage.MainNamespace);
    }

    /// <summary>Ends the sheet; <paramref name="filterRange" /> adds filter buttons over that range.</summary>
    public void EndSheet(string? filterRange)
    {
        _xml.WriteEndElement(); // sheetData

        if (filterRange is not null)
            _xml.WriteRaw($"""<autoFilter ref="{filterRange.Replace("$", string.Empty, StringComparison.Ordinal)}"/>""");

        _xml.WriteEndElement(); // worksheet
        _xml.WriteEndDocument();
    }

    public void BeginRow()
    {
        if (Rows == MaxRows)
            throw new InvalidOperationException($"An Excel sheet holds at most {MaxRows:N0} rows; split the output across files.");

        Rows++;
        _column = 0;
        _rowText = Rows.ToString(CultureInfo.InvariantCulture);
        _xml.WriteRaw("<row r=\"");
        _xml.WriteRaw(_rowText);
        _xml.WriteRaw("\">");
    }

    public void EndRow() => _xml.WriteRaw("</row>");

    /// <summary>The range from A1 to the last cell written, in absolute form (<c>$A$1:$C$10</c>).</summary>
    public string Range(int columnCount) => $"$A$1:${_columnLetters[Math.Max(columnCount, 1) - 1]}${Rows}";

    public void WriteEmpty() => _column++;

    public void WriteInteger(long value)
    {
        if (value is > MaxExactInteger or < -MaxExactInteger)
        {
            WriteText(value.ToString(CultureInfo.InvariantCulture));
            return;
        }

        StartCell(null, 0);
        _ = value.TryFormat(_scratch, out var written, default, CultureInfo.InvariantCulture);
        WriteValue(written);
    }

    public void WriteUnsigned(ulong value)
    {
        if (value > MaxExactInteger)
            WriteText(value.ToString(CultureInfo.InvariantCulture));
        else
            WriteInteger((long)value);
    }

    public void WriteDouble(double value, int style = 0)
    {
        // A workbook cannot hold NaN or infinity as numbers.
        if (!double.IsFinite(value))
        {
            WriteText(ScalarFormatter.Format(value));
            return;
        }

        StartCell(null, style);
        _ = value.TryFormat(_scratch, out var written, "R", CultureInfo.InvariantCulture);
        WriteValue(written);
    }

    public void WriteDecimal(decimal value)
    {
        // Workbooks store numbers as doubles: write a decimal as a number only when it reads back unchanged.
        if (!RoundTripsAsDouble(value))
        {
            WriteText(value.ToString(CultureInfo.InvariantCulture));
            return;
        }

        StartCell(null, 0);
        _ = value.TryFormat(_scratch, out var written, default, CultureInfo.InvariantCulture);
        WriteValue(written);
    }

    private static bool RoundTripsAsDouble(decimal value)
    {
        try
        {
            return Convert.ToDecimal((double)value) == value;
        }
        catch (OverflowException)
        {
            // Near decimal.MaxValue, the nearest double is out of decimal's range.
            return false;
        }
    }

    public void WriteBoolean(bool value)
    {
        StartCell("b", 0);
        _xml.WriteRaw(value ? "<v>1</v></c>" : "<v>0</v></c>");
    }

    public void WriteDate(DateTime value)
    {
        var utc = value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : value;
        WriteDouble(utc.ToOADate(), utc.TimeOfDay == TimeSpan.Zero ? XlsxPackage.DateStyle : XlsxPackage.DateTimeStyle);
    }

    public void WriteText(string? text, int style = 0)
    {
        if (text is null)
        {
            WriteEmpty();
            return;
        }

        if (text.Length > MaxCellText)
        {
            throw new InvalidOperationException(
                $"The value for cell {_columnLetters[_column]}{_rowText} has {text.Length:N0} characters; an Excel cell holds at most {MaxCellText:N0}.");
        }

        StartCell("inlineStr", style);

        // Without xml:space="preserve", Excel trims leading and trailing whitespace (XL-11).
        _xml.WriteRaw(NeedsPreserve(text) ? "<is><t xml:space=\"preserve\">" : "<is><t>");
        _xml.WriteString(Escape(text));
        _xml.WriteRaw("</t></is></c>");
    }

    /// <summary>
    ///     Escapes characters XML 1.0 cannot hold as Excel does, <c>_xHHHH_</c>, and escapes text that already looks like
    ///     such an escape (<c>_x0041_</c> becomes <c>_x005F_x0041_</c>), so readers decode exactly the original.
    /// </summary>
    internal static string Escape(string text)
    {
        var index = FirstToEscape(text);

        if (index < 0)
            return text;

        var builder = new StringBuilder(text.Length + 16);
        _ = builder.Append(text, 0, index);

        for (var i = index; i < text.Length; i++)
        {
            var c = text[i];

            if (char.IsHighSurrogate(c) && i + 1 < text.Length && char.IsLowSurrogate(text[i + 1]))
            {
                _ = builder.Append(c).Append(text[++i]);
                continue;
            }

            if (IsInvalidXml(c) || (c == '_' && LooksLikeEscape(text, i)))
                _ = builder.Append("_x").Append(((int)c).ToString("X4", CultureInfo.InvariantCulture)).Append('_');
            else
                _ = builder.Append(c);
        }

        return builder.ToString();
    }

    private static int FirstToEscape(string text)
    {
        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];

            if (char.IsHighSurrogate(c) && i + 1 < text.Length && char.IsLowSurrogate(text[i + 1]))
            {
                i++;
                continue;
            }

            if (IsInvalidXml(c) || (c == '_' && LooksLikeEscape(text, i)))
                return i;
        }

        return -1;
    }

    private static bool IsInvalidXml(char c) =>
        (c < 0x20 && c is not ('\t' or '\n' or '\r')) || c is '￾' or '￿' || char.IsSurrogate(c);

    private static bool LooksLikeEscape(string text, int i) =>
        i + 6 < text.Length && text[i + 1] is 'x' or 'X' && char.IsAsciiHexDigit(text[i + 2]) && char.IsAsciiHexDigit(text[i + 3])
        && char.IsAsciiHexDigit(text[i + 4]) && char.IsAsciiHexDigit(text[i + 5]) && text[i + 6] == '_';

    private static bool NeedsPreserve(string text) =>
        text.Length > 0 && (char.IsWhiteSpace(text[0]) || char.IsWhiteSpace(text[^1]) || text.AsSpan().IndexOfAny("\t\r\n") >= 0 || text.Contains("  ", StringComparison.Ordinal));

    private void StartCell(string? type, int style)
    {
        _xml.WriteRaw("<c r=\"");
        _xml.WriteRaw(_columnLetters[_column]);
        _xml.WriteRaw(_rowText);
        _xml.WriteRaw(type is null ? "\"" : $"\" t=\"{type}\"");

        if (style != 0)
        {
            _xml.WriteRaw(" s=\"");
            _xml.WriteRaw(style.ToString(CultureInfo.InvariantCulture));
            _xml.WriteRaw("\"");
        }

        _xml.WriteRaw(">");
        _column++;
    }

    private void WriteValue(int length)
    {
        _xml.WriteRaw("<v>");
        _xml.WriteRaw(_scratch, 0, length);
        _xml.WriteRaw("</v></c>");
    }
}

/// <summary>The cached writer for values of type <typeparamref name="T" />, chosen once per type.</summary>
internal static class XlsxCell<T>
{
    public static readonly Action<XlsxSheetWriter, T> Write = (Action<XlsxSheetWriter, T>)XlsxCells.Create(typeof(T));
}

internal static class XlsxCells
{
    public static Delegate Create(Type type)
    {
        if (Nullable.GetUnderlyingType(type) is { } underlying)
            return Generic(nameof(CreateNullable), underlying);

        if (type.IsEnum)
            return Generic(nameof(CreateText), type);

        return type switch
        {
            _ when type == typeof(string) => (Action<XlsxSheetWriter, string?>)((w, v) => w.WriteText(v)),
            _ when type == typeof(bool) => (Action<XlsxSheetWriter, bool>)((w, v) => w.WriteBoolean(v)),
            _ when type == typeof(sbyte) => (Action<XlsxSheetWriter, sbyte>)((w, v) => w.WriteInteger(v)),
            _ when type == typeof(byte) => (Action<XlsxSheetWriter, byte>)((w, v) => w.WriteInteger(v)),
            _ when type == typeof(short) => (Action<XlsxSheetWriter, short>)((w, v) => w.WriteInteger(v)),
            _ when type == typeof(ushort) => (Action<XlsxSheetWriter, ushort>)((w, v) => w.WriteInteger(v)),
            _ when type == typeof(int) => (Action<XlsxSheetWriter, int>)((w, v) => w.WriteInteger(v)),
            _ when type == typeof(uint) => (Action<XlsxSheetWriter, uint>)((w, v) => w.WriteInteger(v)),
            _ when type == typeof(long) => (Action<XlsxSheetWriter, long>)((w, v) => w.WriteInteger(v)),
            _ when type == typeof(ulong) => (Action<XlsxSheetWriter, ulong>)((w, v) => w.WriteUnsigned(v)),
            _ when type == typeof(float) => (Action<XlsxSheetWriter, float>)((w, v) => w.WriteDouble(double.Parse(v.ToString("R", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture))),
            _ when type == typeof(double) => (Action<XlsxSheetWriter, double>)((w, v) => w.WriteDouble(v)),
            _ when type == typeof(decimal) => (Action<XlsxSheetWriter, decimal>)((w, v) => w.WriteDecimal(v)),
            _ when type == typeof(DateTime) => (Action<XlsxSheetWriter, DateTime>)((w, v) => w.WriteDate(v)),
            _ when type == typeof(DateTimeOffset) => (Action<XlsxSheetWriter, DateTimeOffset>)((w, v) => w.WriteDate(v.UtcDateTime)),
            _ when type == typeof(DateOnly) => (Action<XlsxSheetWriter, DateOnly>)((w, v) => w.WriteDate(v.ToDateTime(TimeOnly.MinValue))),
            _ when type == typeof(TimeOnly) => (Action<XlsxSheetWriter, TimeOnly>)((w, v) => w.WriteDouble(v.ToTimeSpan().TotalDays, XlsxPackage.TimeStyle)),

            // No lossless cell type: written as the text the connectors parse back.
            _ => Generic(nameof(CreateText), type),
        };
    }

    private static Delegate Generic(string factory, Type type) =>
        (Delegate)typeof(XlsxCells).GetMethod(factory, BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(type).Invoke(null, null)!;

    private static Action<XlsxSheetWriter, T?> CreateNullable<T>()
        where T : struct
    {
        var inner = XlsxCell<T>.Write;
        return (writer, value) =>
        {
            if (value is { } v)
                inner(writer, v);
            else
                writer.WriteEmpty();
        };
    }

    // TimeSpan, Guid, char, enums and byte[]; ScalarFormatter throws for types that are not scalars.
    private static Action<XlsxSheetWriter, T> CreateText<T>()
    {
        var format = ScalarFormatter.GetFormatter<T>();
        return (writer, value) => writer.WriteText(format(value, CultureInfo.InvariantCulture));
    }
}
