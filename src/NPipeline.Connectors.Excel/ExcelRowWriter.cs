using NPipeline.Connectors.Excel.Xlsx;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Excel;

/// <summary>
///     Writes the cells of one row, in order. A manual writer passed to <see cref="ExcelSinkNode{T}" /> calls
///     <see cref="Write{TValue}" /> once per column.
/// </summary>
/// <remarks>
///     Numbers, booleans and dates are written as typed cells, with date formats so Excel shows dates as dates.
///     <c>long</c>, <c>ulong</c> and <c>decimal</c> values that a workbook's doubles cannot hold exactly are written as
///     text. <c>TimeSpan</c>, <c>Guid</c>, enums and <c>byte[]</c> are written as text; <c>null</c> is an empty cell.
/// </remarks>
public sealed class ExcelRowWriter : IFieldWriter
{
    private readonly XlsxSheetWriter _sheet;

    internal ExcelRowWriter(XlsxSheetWriter sheet)
    {
        _sheet = sheet;
    }

    /// <summary>Writes the next cell.</summary>
    /// <exception cref="NotSupportedException"><typeparamref name="TValue" /> is not a scalar type.</exception>
    public void Write<TValue>(TValue value) => XlsxCell<TValue>.Write(_sheet, value);

    /// <inheritdoc />
    void IFieldWriter.WriteValue<TValue>(int ordinal, TValue value) => XlsxCell<TValue>.Write(_sheet, value);
}
