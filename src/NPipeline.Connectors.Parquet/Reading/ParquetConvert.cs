using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Parquet.Reading;

/// <summary>
///     Converts a column's storage value to a member's type, strictly. Built once per pair of types, so reading the
///     columns this connector writes converts without boxing; anything else (schema evolution, other writers' types)
///     goes through <see cref="ScalarConverter" />.
/// </summary>
internal static class ParquetConvert<TFrom, TTo>
{
    public static readonly Func<TFrom, TTo> Func = ParquetConverters.Create<TFrom, TTo>();
}

internal static class ParquetConverters
{
    public static Func<TFrom, TTo> Create<TFrom, TTo>()
    {
        var from = typeof(TFrom);
        var to = typeof(TTo);

        if (from == to)
            return (Func<TFrom, TTo>)(object)(Func<TFrom, TFrom>)(static value => value);

        // U? to U (a list's items), or U? to V: unwrap without boxing; callers check for null first.
        if (Nullable.GetUnderlyingType(from) is { } fromUnderlying && Nullable.GetUnderlyingType(to) is null)
        {
            if (fromUnderlying == to)
                return Compile<TFrom, TTo>(value => Expression.Convert(value, to));

            var converter = typeof(ParquetConvert<,>).MakeGenericType(fromUnderlying, to).GetField(nameof(ParquetConvert<int, int>.Func))!.GetValue(null)!;
            var unwrap = typeof(ParquetConverters).GetMethod(nameof(Unwrap), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(fromUnderlying, to);
            return (Func<TFrom, TTo>)unwrap.Invoke(null, [converter])!;
        }

        if (Nullable.GetUnderlyingType(to) is { } underlying)
        {
            // TFrom to U?: convert to U, then wrap, without boxing.
            if (underlying == from)
                return Compile<TFrom, TTo>(value => Expression.Convert(value, to));

            var inner = typeof(ParquetConvert<,>).MakeGenericType(from, underlying).GetField(nameof(ParquetConvert<int, int>.Func))!.GetValue(null)!;
            var wrap = typeof(ParquetConverters).GetMethod(nameof(Wrap), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(from, underlying);
            return (Func<TFrom, TTo>)wrap.Invoke(null, [inner])!;
        }

        Delegate? special = (from, to) switch
        {
            _ when from == typeof(string) => (Func<string?, TTo>)(static text => text is null
                ? throw new FieldConversionException(typeof(TTo), null, "the value is missing")
                : ScalarParser.Parse<TTo>(text)),
            _ when from == typeof(DateTime) && to == typeof(DateTimeOffset) => (Func<DateTime, DateTimeOffset>)(static value =>
                new DateTimeOffset(value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : DateTime.SpecifyKind(value, DateTimeKind.Utc))),
            _ when from == typeof(DateTime) && to == typeof(DateOnly) => (Func<DateTime, DateOnly>)(static value => value.TimeOfDay == TimeSpan.Zero
                ? DateOnly.FromDateTime(value)
                : throw new FieldConversionException(typeof(DateOnly), value.ToString("O", CultureInfo.InvariantCulture), "the value has a time of day")),
            _ when from == typeof(long) && to == typeof(TimeSpan) => (Func<long, TimeSpan>)(static ticks => TimeSpan.FromTicks(ticks)),
            _ when to == typeof(string) => (Func<TFrom, string?>)(static value => ScalarFormatter.Format(value)),
            _ => null,
        };

        return special as Func<TFrom, TTo> ?? (static value => ScalarConverter.Convert<TTo>(value));
    }

    private static Func<TFrom, TTo> Compile<TFrom, TTo>(Func<ParameterExpression, Expression> body)
    {
        var value = Expression.Parameter(typeof(TFrom), "value");
        return Expression.Lambda<Func<TFrom, TTo>>(body(value), value).Compile();
    }

    private static Func<TUnderlying?, TTo> Unwrap<TUnderlying, TTo>(Func<TUnderlying, TTo> inner)
        where TUnderlying : struct =>
        value => inner(value!.Value);

    private static Func<TFrom, TUnderlying?> Wrap<TFrom, TUnderlying>(Func<TFrom, TUnderlying> inner)
        where TUnderlying : struct =>
        value => inner(value);
}
