using System.Globalization;
using System.Reflection;

namespace NPipeline.Connectors.Mapping;

/// <summary>Formats a scalar value as text. Returns <c>null</c> for a <c>null</c> value.</summary>
/// <typeparam name="T">The value's type.</typeparam>
public delegate string? ScalarFormat<in T>(T value, IFormatProvider provider);

/// <summary>
///     Formats scalar values as text that <see cref="ScalarParser" /> reads back unchanged: the writing half of the
///     connectors' text formats.
/// </summary>
/// <remarks>
///     <list type="bullet">
///         <item>Numbers use the invariant culture unless a provider is given, with no thousands separators; floating point uses the shortest text that round-trips.</item>
///         <item><see cref="DateTime" />, <see cref="DateTimeOffset" />, <see cref="DateOnly" /> and <see cref="TimeOnly" /> use the ISO 8601 round-trip format (<c>O</c>), whatever the provider; <see cref="TimeSpan" /> uses <c>c</c>.</item>
///         <item>Enums are written by name, <see cref="bool" /> as <c>true</c> or <c>false</c>, <see cref="Guid" /> in the <c>D</c> format, and <c>byte[]</c> as base64.</item>
///     </list>
/// </remarks>
public static class ScalarFormatter
{
    /// <summary>Formats <paramref name="value" />, or returns <c>null</c> when it is <c>null</c>.</summary>
    /// <exception cref="NotSupportedException"><typeparamref name="T" /> is not a scalar type.</exception>
    public static string? Format<T>(T value, IFormatProvider? provider = null) =>
        ScalarFormatter<T>.Formatter(value, provider ?? CultureInfo.InvariantCulture);

    /// <summary>Returns the cached formatter for <typeparamref name="T" />, for callers that format many values.</summary>
    public static ScalarFormat<T> GetFormatter<T>() => ScalarFormatter<T>.Formatter;
}

internal static class ScalarFormatter<T>
{
    public static readonly ScalarFormat<T> Formatter = (ScalarFormat<T>)ScalarFormatters.Create(typeof(T));
}

internal static class ScalarFormatters
{
    public static Delegate Create(Type type)
    {
        if (Nullable.GetUnderlyingType(type) is { } underlying)
            return Generic(nameof(CreateNullable), underlying);

        if (type.IsEnum)
            return Generic(nameof(CreateEnum), type);

        return type switch
        {
            _ when type == typeof(string) => (ScalarFormat<string?>)((v, _) => v),
            _ when type == typeof(bool) => (ScalarFormat<bool>)((v, _) => v ? "true" : "false"),
            _ when type == typeof(char) => (ScalarFormat<char>)((v, _) => v.ToString()),
            _ when type == typeof(byte) => (ScalarFormat<byte>)((v, p) => v.ToString(p)),
            _ when type == typeof(sbyte) => (ScalarFormat<sbyte>)((v, p) => v.ToString(p)),
            _ when type == typeof(short) => (ScalarFormat<short>)((v, p) => v.ToString(p)),
            _ when type == typeof(ushort) => (ScalarFormat<ushort>)((v, p) => v.ToString(p)),
            _ when type == typeof(int) => (ScalarFormat<int>)((v, p) => v.ToString(p)),
            _ when type == typeof(uint) => (ScalarFormat<uint>)((v, p) => v.ToString(p)),
            _ when type == typeof(long) => (ScalarFormat<long>)((v, p) => v.ToString(p)),
            _ when type == typeof(ulong) => (ScalarFormat<ulong>)((v, p) => v.ToString(p)),

            // .NET Core 3.0 and later: the default format is the shortest text that round-trips.
            _ when type == typeof(float) => (ScalarFormat<float>)((v, p) => v.ToString(p)),
            _ when type == typeof(double) => (ScalarFormat<double>)((v, p) => v.ToString(p)),
            _ when type == typeof(decimal) => (ScalarFormat<decimal>)((v, p) => v.ToString(p)),
            _ when type == typeof(DateTime) => (ScalarFormat<DateTime>)((v, _) => v.ToString("O", CultureInfo.InvariantCulture)),
            _ when type == typeof(DateTimeOffset) => (ScalarFormat<DateTimeOffset>)((v, _) => v.ToString("O", CultureInfo.InvariantCulture)),
            _ when type == typeof(DateOnly) => (ScalarFormat<DateOnly>)((v, _) => v.ToString("O", CultureInfo.InvariantCulture)),
            _ when type == typeof(TimeOnly) => (ScalarFormat<TimeOnly>)((v, _) => v.ToString("O", CultureInfo.InvariantCulture)),
            _ when type == typeof(TimeSpan) => (ScalarFormat<TimeSpan>)((v, _) => v.ToString("c", CultureInfo.InvariantCulture)),
            _ when type == typeof(Guid) => (ScalarFormat<Guid>)((v, _) => v.ToString("D")),
            _ when type == typeof(byte[]) => (ScalarFormat<byte[]?>)((v, _) => v is null ? null : Convert.ToBase64String(v)),
            _ => Generic(nameof(CreateUnsupported), type),
        };
    }

    private static Delegate Generic(string factory, Type type) =>
        (Delegate)typeof(ScalarFormatters)
            .GetMethod(factory, BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(type)
            .Invoke(null, null)!;

    // Built into a throwing formatter, so asking for an unsupported type fails where it is used, with the type.
    private static ScalarFormat<T> CreateUnsupported<T>() =>
        (_, _) => throw new NotSupportedException($"{typeof(T).Name} is not a scalar type the connectors can format as text.");

    private static ScalarFormat<T?> CreateNullable<T>()
        where T : struct
    {
        var inner = ScalarFormatter<T>.Formatter;
        return (value, provider) => value is { } v ? inner(v, provider) : null;
    }

    private static ScalarFormat<TEnum> CreateEnum<TEnum>()
        where TEnum : struct, Enum =>
        (value, _) => value.ToString();
}
