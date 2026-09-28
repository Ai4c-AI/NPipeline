using System.IO.Compression;
using System.Security;
using System.Text;

namespace NPipeline.Connectors.Excel.Xlsx;

/// <summary>
///     The parts of a one-sheet XLSX package other than the sheet itself. An XLSX file is a zip of a few small XML parts
///     plus one part per sheet, so writing them directly streams where an object-model writer holds the whole workbook.
/// </summary>
internal static class XlsxPackage
{
    public const string SheetPath = "xl/worksheets/sheet1.xml";

    public const string MainNamespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";

    /// <summary>Cell styles in <see cref="Styles" />: 0 is general.</summary>
    public const int DateStyle = 1;

    public const int DateTimeStyle = 2;

    public const int TimeStyle = 3;

    public const int HeaderStyle = 4;

    private const string Declaration = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>""";

    private const string ContentTypes = Declaration + """
        <Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/></Types>
        """;

    private const string PackageRelationships = Declaration + """
        <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>
        """;

    private const string WorkbookRelationships = Declaration + """
        <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>
        """;

    // Styles 1-3 give dates a number format, so Excel shows them as dates and readers return them as dates (XL-5).
    private const string Styles = Declaration + """
        <styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><numFmts count="2"><numFmt numFmtId="164" formatCode="yyyy-mm-dd"/><numFmt numFmtId="165" formatCode="yyyy-mm-dd hh:mm:ss"/></numFmts><fonts count="2"><font><sz val="11"/><name val="Calibri"/><family val="2"/></font><font><b/><sz val="11"/><name val="Calibri"/><family val="2"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="5"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="164" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/><xf numFmtId="165" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/><xf numFmtId="21" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/><xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>
        """;

    /// <summary>Writes the parts known before the sheet.</summary>
    public static void WriteLeadingParts(ZipArchive archive)
    {
        WritePart(archive, "[Content_Types].xml", ContentTypes);
        WritePart(archive, "_rels/.rels", PackageRelationships);
        WritePart(archive, "xl/_rels/workbook.xml.rels", WorkbookRelationships);
        WritePart(archive, "xl/styles.xml", Styles);
    }

    /// <summary>Writes the workbook part, which needs the filter range known only after the sheet.</summary>
    public static void WriteWorkbook(ZipArchive archive, string sheetName, string? filterRange)
    {
        var name = SecurityElement.Escape(sheetName);

        var definedNames = filterRange is null
            ? string.Empty
            : $"""<definedNames><definedName name="_xlnm._FilterDatabase" localSheetId="0" hidden="1">'{name.Replace("'", "''", StringComparison.Ordinal)}'!{filterRange}</definedName></definedNames>""";

        WritePart(archive, "xl/workbook.xml", Declaration +
            $"""<workbook xmlns="{MainNamespace}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="{name}" sheetId="1" r:id="rId1"/></sheets>{definedNames}</workbook>""");
    }

    private static void WritePart(ZipArchive archive, string path, string xml)
    {
        using var stream = archive.CreateEntry(path, CompressionLevel.Fastest).Open();
        stream.Write(Encoding.UTF8.GetBytes(xml));
    }
}
