using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using NPipeline.Connectors.Errors;

namespace NPipeline.Connectors.Mapping;

/// <summary>
///     Strict conversion of an object value (an Excel cell, a Parquet value) to a scalar type. Text is parsed with
///     <see cref="ScalarParser" />; other values convert only when no information is lost. Anything else throws
///     <see cref="FieldConversionException" />, and never becomes a default value.
/// </summary>
/// <remarks>
///     <list type="bullet">
///         <item>A floating-point value converts to an integer only when it is whole and in range (spreadsheets store integers as doubles).</item>
///         <item>A <see cref="double" /> converts to a date or time as an OLE Automation date, as spreadsheets store them.</item>
///         <item>A <see cref="DateTime" /> that is not <see cref="DateTimeKind.Local" /> converts to a <see cref="DateTimeOffset" /> at UTC.</item>
///         <item>An integer converts to an enum only when the enum defines it.</item>
///         <item><c>null</c> and <see cref="DBNull" /> convert to <c>null</c> for reference and nullable types, and throw for other value types.</item>
///     </list>
/// </remarks>
public static class ScalarConverter
{
    private static readonly ConcurrentDictionary<Type, Func<string, IFormatProvider, object?>> TextParsers = new();

    /// <summary>Converts <paramref name="value" /> to <typeparamref name="T" />.</summary>
    /// <exception cref="FieldConversionException">The value cannot be converted without losing information.</exception>
    public static T Convert<T>(object? value, IFormatProvider? provider = null)
    {
        if (value is null or DBNull)
        {
            return default(T) is null
                ? default!
                : throw new FieldConversionException(typeof(T), null, "the value is missing");
        }

        if (value is T typed)
            return typed;

        var target = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
        return (T)ConvertCore(value, target, provider ?? CultureInfo.InvariantCulture);
    }

    private static object ConvertCore(object value, Type target, IFormatProvider provider)
    {
        switch (value)
        {
            case string text:
                return ParseText(text, target, provider);
            case ReadOnlyMemory<char> memory:
                return ParseText(memory.ToString(), target, provider);
            case ReadOnlyMemory<byte> bytes when target == typeof(byte[]):
                return bytes.ToArray();
        }

        if (target == typeof(string))
            return FormatAsText(value, provider);

        if (target.IsEnum)
            return ToEnum(value, target);

        if (IsNumeric(target))
            return ToNumber(value, target);

        if (target == typeof(bool))
        {
            return value switch
            {
                _ when IsNumeric(value.GetType()) && ToNumber(value, typeof(decimal)) is decimal d && d is 0 or 1 => d == 1,
                _ => Fail(value, target),
            };
        }

        if (target == typeof(DateTime))
        {
            return value switch
            {
                DateTimeOffset dto => dto.UtcDateTime,
                DateOnly date => date.ToDateTime(TimeOnly.MinValue),
                double oaDate => FromOADate(oaDate, target),
                _ => Fail(value, target),
            };
        }

        if (target == typeof(DateTimeOffset))
        {
            return value switch
            {
                DateTime { Kind: DateTimeKind.Local } local => new DateTimeOffset(local),
                DateTime dateTime => new DateTimeOffset(DateTime.SpecifyKind(dateTime, DateTimeKind.Utc)),
                double oaDate => new DateTimeOffset(DateTime.SpecifyKind(FromOADate(oaDate, target), DateTimeKind.Utc)),
                _ => Fail(value, target),
            };
        }

        if (target == typeof(DateOnly))
        {
            var dateTime = value switch
            {
                DateTime d => d,
                DateTimeOffset dto => dto.DateTime,
                double oaDate => FromOADate(oaDate, target),
                _ => (DateTime?)null,
            };

            return dateTime is { } midnight && midnight.TimeOfDay == TimeSpan.Zero
                ? DateOnly.FromDateTime(midnight)
                : Fail(value, target, dateTime is null ? null : "the value has a time of day");
        }

        if (target == typeof(TimeOnly))
        {
            return value switch
            {
                TimeSpan span when span >= TimeSpan.Zero && span < TimeSpan.FromDays(1) => TimeOnly.FromTimeSpan(span),
                DateTime dateTime => TimeOnly.FromDateTime(dateTime),
                double fraction when fraction is >= 0 and < 1 => TimeOnly.FromTimeSpan(TimeSpan.FromDays(fraction)),
                _ => Fail(value, target),
            };
        }

        if (target == typeof(TimeSpan))
        {
            return value switch
            {
                TimeOnly time => time.ToTimeSpan(),
                double days => TimeSpan.FromDays(days),
                _ => Fail(value, target),
            };
        }

        if (target == typeof(Guid) && value is byte[] { Length: 16 } guidBytes)
            return new Guid(guidBytes);

        return Fail(value, target);
    }

    private static object ParseText(string text, Type target, IFormatProvider provider) =>
        TextParsers.GetOrAdd(target, static type =>
            typeof(ScalarConverter)
                .GetMethod(nameof(ParseBoxed), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(type)
                .CreateDelegate<Func<string, IFormatProvider, object?>>())(text, provider)!;

    private static object? ParseBoxed<T>(string text, IFormatProvider provider) => ScalarParser<T>.Parser(text, provider);

    private static string FormatAsText(object value, IFormatProvider provider) =>
        value switch
        {
            DateTime dateTime => dateTime.ToString("O", CultureInfo.InvariantCulture),
            DateTimeOffset dto => dto.ToString("O", CultureInfo.InvariantCulture),
            byte[] bytes => System.Convert.ToBase64String(bytes),
            IFormattable formattable => formattable.ToString(null, provider),
            _ => value.ToString() ?? string.Empty,
        };

    private static bool IsNumeric(Type type) =>
        Type.GetTypeCode(type) is TypeCode.SByte or TypeCode.Byte or TypeCode.Int16 or TypeCode.UInt16 or TypeCode.Int32 or TypeCode.UInt32
            or TypeCode.Int64 or TypeCode.UInt64 or TypeCode.Single or TypeCode.Double or TypeCode.Decimal
        && !type.IsEnum;

    private static object ToNumber(object value, Type target)
    {
        if (!IsNumeric(value.GetType()))
            return Fail(value, target);

        var isIntegralTarget = Type.GetTypeCode(target) is not (TypeCode.Single or TypeCode.Double or TypeCode.Decimal);

        // Convert.ChangeType rounds a fractional value into an integer; only whole values are the same number.
        if (isIntegralTarget && value is double or float && Math.Truncate(System.Convert.ToDouble(value, CultureInfo.InvariantCulture)) != System.Convert.ToDouble(value, CultureInfo.InvariantCulture))
            return Fail(value, target, "the value is not a whole number");

        if (isIntegralTarget && value is decimal m && decimal.Truncate(m) != m)
            return Fail(value, target, "the value is not a whole number");

        try
        {
            return System.Convert.ChangeType(value, target, CultureInfo.InvariantCulture);
        }
        catch (OverflowException ex)
        {
            throw new FieldConversionException(target, FormatAsText(value, CultureInfo.InvariantCulture), "the value is out of range", ex);
        }
    }

    private static object ToEnum(object value, Type target)
    {
        if (!IsNumeric(value.GetType()) || ToNumber(value, typeof(long)) is not long number)
            return Fail(value, target);

        var result = Enum.ToObject(target, number);

        return target.IsDefined(typeof(FlagsAttribute), false) || Enum.IsDefined(target, result)
            ? result
            : Fail(value, target, "not a defined value");
    }

    private static DateTime FromOADate(double oaDate, Type target)
    {
        try
        {
            return DateTime.FromOADate(oaDate);
        }
        catch (ArgumentException ex)
        {
            throw new FieldConversionException(target, oaDate.ToString("R", CultureInfo.InvariantCulture), "not a valid OLE Automation date", ex);
        }
    }

    private static object Fail(object value, Type target, string? reason = null) =>
        throw new FieldConversionException(
            target,
            FormatAsText(value, CultureInfo.InvariantCulture),
            reason ?? $"no lossless conversion from {value.GetType().Name}");
}
