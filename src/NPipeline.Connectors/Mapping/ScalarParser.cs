using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Reflection;
using NPipeline.Connectors.Errors;

namespace NPipeline.Connectors.Mapping;

/// <summary>Parses text into a scalar of type <typeparamref name="T" />.</summary>
/// <typeparam name="T">The target type.</typeparam>
/// <param name="text">The text to parse.</param>
/// <param name="provider">The culture or format provider.</param>
public delegate T SpanParser<out T>(ReadOnlySpan<char> text, IFormatProvider provider);

/// <summary>
///     Strict, culture-invariant parsing of text into the scalar types in <see cref="TypeClassifier" />. Bad input throws
///     <see cref="FieldConversionException" />; it never becomes a default value.
/// </summary>
/// <remarks>
///     <list type="bullet">
///         <item>Numbers use the invariant culture unless a provider is given, and never accept thousands separators, so <c>1,5</c> is an error rather than 15.</item>
///         <item>A <see cref="DateTime" /> or <see cref="DateTimeOffset" /> without an offset is UTC, whatever the host's time zone.</item>
///         <item>Enums accept names (case-insensitive) or the numbers of defined values.</item>
///         <item><c>byte[]</c> is base64. <see cref="bool" /> accepts <c>true</c>, <c>false</c>, <c>1</c> and <c>0</c>.</item>
///         <item>For <see cref="Nullable{T}" />, empty or blank text is <c>null</c>. For <see cref="string" />, text is returned as is.</item>
///     </list>
/// </remarks>
public static class ScalarParser
{
    /// <summary>Parses <paramref name="text" /> as <typeparamref name="T" />.</summary>
    /// <exception cref="FieldConversionException">The text is not a valid <typeparamref name="T" />.</exception>
    /// <exception cref="NotSupportedException"><typeparamref name="T" /> is not a scalar type.</exception>
    public static T Parse<T>(ReadOnlySpan<char> text, IFormatProvider? provider = null) =>
        ScalarParser<T>.Parser(text, provider ?? CultureInfo.InvariantCulture);

    /// <summary>Parses <paramref name="text" /> as <typeparamref name="T" />, returning <c>false</c> instead of throwing for invalid text.</summary>
    public static bool TryParse<T>(ReadOnlySpan<char> text, IFormatProvider? provider, [MaybeNullWhen(false)] out T value)
    {
        try
        {
            value = Parse<T>(text, provider)!;
            return true;
        }
        catch (FieldConversionException)
        {
            value = default;
            return false;
        }
    }

    /// <summary>Returns the cached parser for <typeparamref name="T" />, for callers that parse many values.</summary>
    public static SpanParser<T> GetParser<T>() => ScalarParser<T>.Parser;
}

internal static class ScalarParser<T>
{
    public static readonly SpanParser<T> Parser = (SpanParser<T>)ScalarParsers.Create(typeof(T));
}

internal static class ScalarParsers
{
    private const NumberStyles IntegerStyle = NumberStyles.Integer;

    // Float, not Number: AllowThousands would read "1,5" as 15 under the invariant culture.
    private const NumberStyles RealStyle = NumberStyles.Float;

    private const DateTimeStyles UtcStyle = DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal;

    public static Delegate Create(Type type)
    {
        if (Nullable.GetUnderlyingType(type) is { } underlying)
            return Generic(nameof(CreateNullable), underlying);

        if (type.IsEnum)
            return Generic(nameof(CreateEnum), type);

        return type switch
        {
            _ when type == typeof(string) => (SpanParser<string>)((text, _) => text.ToString()),
            _ when type == typeof(bool) => (SpanParser<bool>)ParseBoolean,
            _ when type == typeof(char) => (SpanParser<char>)ParseChar,
            _ when type == typeof(byte) => (SpanParser<byte>)((t, p) => byte.TryParse(t, IntegerStyle, p, out var v) ? v : Fail<byte>(t)),
            _ when type == typeof(sbyte) => (SpanParser<sbyte>)((t, p) => sbyte.TryParse(t, IntegerStyle, p, out var v) ? v : Fail<sbyte>(t)),
            _ when type == typeof(short) => (SpanParser<short>)((t, p) => short.TryParse(t, IntegerStyle, p, out var v) ? v : Fail<short>(t)),
            _ when type == typeof(ushort) => (SpanParser<ushort>)((t, p) => ushort.TryParse(t, IntegerStyle, p, out var v) ? v : Fail<ushort>(t)),
            _ when type == typeof(int) => (SpanParser<int>)((t, p) => int.TryParse(t, IntegerStyle, p, out var v) ? v : Fail<int>(t)),
            _ when type == typeof(uint) => (SpanParser<uint>)((t, p) => uint.TryParse(t, IntegerStyle, p, out var v) ? v : Fail<uint>(t)),
            _ when type == typeof(long) => (SpanParser<long>)((t, p) => long.TryParse(t, IntegerStyle, p, out var v) ? v : Fail<long>(t)),
            _ when type == typeof(ulong) => (SpanParser<ulong>)((t, p) => ulong.TryParse(t, IntegerStyle, p, out var v) ? v : Fail<ulong>(t)),
            _ when type == typeof(float) => (SpanParser<float>)((t, p) => float.TryParse(t, RealStyle, p, out var v) ? v : Fail<float>(t)),
            _ when type == typeof(double) => (SpanParser<double>)((t, p) => double.TryParse(t, RealStyle, p, out var v) ? v : Fail<double>(t)),
            _ when type == typeof(decimal) => (SpanParser<decimal>)((t, p) => decimal.TryParse(t, RealStyle, p, out var v) ? v : Fail<decimal>(t)),
            _ when type == typeof(DateTime) => (SpanParser<DateTime>)((t, p) => DateTime.TryParse(t, p, UtcStyle, out var v) ? v : Fail<DateTime>(t)),
            _ when type == typeof(DateTimeOffset) => (SpanParser<DateTimeOffset>)((t, p) =>
                DateTimeOffset.TryParse(t, p, DateTimeStyles.AssumeUniversal, out var v) ? v : Fail<DateTimeOffset>(t)),
            _ when type == typeof(DateOnly) => (SpanParser<DateOnly>)ParseDateOnly,
            _ when type == typeof(TimeOnly) => (SpanParser<TimeOnly>)((t, p) => TimeOnly.TryParse(t, p, DateTimeStyles.None, out var v) ? v : Fail<TimeOnly>(t)),
            _ when type == typeof(TimeSpan) => (SpanParser<TimeSpan>)((t, p) => TimeSpan.TryParse(t, p, out var v) ? v : Fail<TimeSpan>(t)),
            _ when type == typeof(Guid) => (SpanParser<Guid>)((t, _) => Guid.TryParse(t, out var v) ? v : Fail<Guid>(t)),
            _ when type == typeof(byte[]) => (SpanParser<byte[]>)ParseBase64,
            _ => Unsupported(type),
        };
    }

    internal static T Fail<T>(ReadOnlySpan<char> text, string? reason = null) =>
        throw new FieldConversionException(typeof(T), text.ToString(), reason);

    private static bool IsBlank(ReadOnlySpan<char> text) => text.IsEmpty || text.IsWhiteSpace();

    private static Delegate Generic(string factory, Type type) =>
        (Delegate)typeof(ScalarParsers)
            .GetMethod(factory, BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(type)
            .Invoke(null, null)!;

    private static Delegate Unsupported(Type type)
    {
        // Built lazily into a throwing parser, so asking for an unsupported type fails where it is used, with the type.
        var method = typeof(ScalarParsers).GetMethod(nameof(CreateUnsupported), BindingFlags.NonPublic | BindingFlags.Static)!;
        return (Delegate)method.MakeGenericMethod(type).Invoke(null, null)!;
    }

    private static SpanParser<T> CreateUnsupported<T>() =>
        (_, _) => throw new NotSupportedException($"{typeof(T).Name} is not a scalar type the connectors can parse from text.");

    private static SpanParser<T?> CreateNullable<T>()
        where T : struct
    {
        var inner = ScalarParser<T>.Parser;
        return (text, provider) => IsBlank(text) ? null : inner(text, provider);
    }

    private static SpanParser<TEnum> CreateEnum<TEnum>()
        where TEnum : struct, Enum
    {
        var isFlags = typeof(TEnum).IsDefined(typeof(FlagsAttribute), false);

        return (text, _) =>
        {
            if (!Enum.TryParse<TEnum>(text.Trim(), true, out var value))
                return Fail<TEnum>(text);

            // Enum.TryParse accepts any number; only defined values (or flag combinations) are valid data.
            return isFlags || Enum.IsDefined(value)
                ? value
                : Fail<TEnum>(text, "not a defined value");
        };
    }

    private static bool ParseBoolean(ReadOnlySpan<char> text, IFormatProvider provider)
    {
        var trimmed = text.Trim();

        if (bool.TryParse(trimmed, out var value))
            return value;

        return trimmed switch
        {
            "1" => true,
            "0" => false,
            _ => Fail<bool>(text),
        };
    }

    private static char ParseChar(ReadOnlySpan<char> text, IFormatProvider provider) =>
        text.Length == 1
            ? text[0]
            : Fail<char>(text, "expected exactly one character");

    private static DateOnly ParseDateOnly(ReadOnlySpan<char> text, IFormatProvider provider)
    {
        if (DateOnly.TryParse(text, provider, DateTimeStyles.None, out var date))
            return date;

        // Many writers emit dates as midnight timestamps ("2026-01-02 00:00:00"); accept those, but not other times.
        if (DateTime.TryParse(text, provider, UtcStyle, out var dateTime))
        {
            return dateTime.TimeOfDay == TimeSpan.Zero
                ? DateOnly.FromDateTime(dateTime)
                : Fail<DateOnly>(text, "the value has a time of day");
        }

        return Fail<DateOnly>(text);
    }

    private static byte[] ParseBase64(ReadOnlySpan<char> text, IFormatProvider provider)
    {
        var buffer = new byte[(text.Length * 3 / 4) + 3];

        return Convert.TryFromBase64Chars(text, buffer, out var written)
            ? buffer.AsSpan(0, written).ToArray()
            : Fail<byte[]>(text, "not base64");
    }
}
