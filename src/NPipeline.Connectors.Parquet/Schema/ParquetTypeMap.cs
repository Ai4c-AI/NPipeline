using System.Reflection;
using NPipeline.Connectors.Parquet.Attributes;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet.Schema;

/// <summary>
///     Which Parquet column a member type becomes, and which CLR type holds its values in a row group (the storage type).
///     One table for writing schemas, for writers and for readers, so they cannot disagree.
/// </summary>
internal static class ParquetTypeMap
{
    /// <summary>Decimal precision and scale when a member has no <see cref="ParquetDecimalAttribute" />.</summary>
    public const int DefaultPrecision = 38;

    public const int DefaultScale = 18;

    private const string ElementName = "element";

    /// <summary>Whether <paramref name="type" /> (or its nullable form) is a single value Parquet can hold.</summary>
    public static bool IsScalar(Type type)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;

        return t.IsEnum || t == typeof(bool) || t == typeof(sbyte) || t == typeof(byte) || t == typeof(short) || t == typeof(ushort)
               || t == typeof(int) || t == typeof(uint) || t == typeof(long) || t == typeof(ulong) || t == typeof(float) || t == typeof(double)
               || t == typeof(decimal) || t == typeof(string) || t == typeof(char) || t == typeof(byte[]) || t == typeof(Guid)
               || t == typeof(DateTime) || t == typeof(DateTimeOffset) || t == typeof(DateOnly) || t == typeof(TimeOnly) || t == typeof(TimeSpan);
    }

    /// <summary>
    ///     The element type when <paramref name="type" /> is a list of scalars: an array (other than <c>byte[]</c>), or
    ///     <see cref="List{T}" />, <see cref="IList{T}" />, <see cref="IReadOnlyList{T}" />, <see cref="ICollection{T}" />,
    ///     <see cref="IReadOnlyCollection{T}" /> or <see cref="IEnumerable{T}" />.
    /// </summary>
    public static Type? ListElement(Type type)
    {
        Type? element = null;

        if (type.IsArray && type != typeof(byte[]) && type.GetArrayRank() == 1)
            element = type.GetElementType();
        else if (type.IsGenericType)
        {
            var definition = type.GetGenericTypeDefinition();

            if (definition == typeof(List<>) || definition == typeof(IList<>) || definition == typeof(IReadOnlyList<>) || definition == typeof(ICollection<>)
                || definition == typeof(IReadOnlyCollection<>) || definition == typeof(IEnumerable<>))
                element = type.GetGenericArguments()[0];
        }

        return element is not null && IsScalar(element) ? element : null;
    }

    /// <summary>Whether a member of <paramref name="type" /> can be written to and read from a column.</summary>
    public static bool IsSupported(Type type) => IsScalar(type) || ListElement(type) is not null;

    /// <summary>The column for a member.</summary>
    public static Field CreateField(string name, Type memberType, MemberInfo? member)
    {
        if (ListElement(memberType) is { } element)
            return new ListField(name, CreateScalarField(ElementName, element, IsNullable(element), member));

        return CreateScalarField(name, memberType, IsNullable(memberType), member);
    }

    /// <summary>The CLR type a row group holds for a scalar member type when writing.</summary>
    public static Type StorageType(Type scalarType)
    {
        var t = Nullable.GetUnderlyingType(scalarType) ?? scalarType;

        return t switch
        {
            _ when t.IsEnum || t == typeof(char) => typeof(string),
            _ when t == typeof(DateTimeOffset) => typeof(DateTime),
            _ when t == typeof(TimeOnly) || t == typeof(TimeSpan) => typeof(long),
            _ => t,
        };
    }

    /// <summary>
    ///     The CLR type a column's values are held as. A schema read from a file reports text and binary columns as
    ///     <see cref="ReadOnlyMemory{T}" />; this connector holds them as <see cref="string" /> and <see cref="byte" /> arrays.
    /// </summary>
    public static Type ClrType(DataField field) =>
        field.ClrType == typeof(ReadOnlyMemory<char>) ? typeof(string)
        : field.ClrType == typeof(ReadOnlyMemory<byte>) ? typeof(byte[])
        : field.ClrType;

    public static bool IsNullable(Type type) => !type.IsValueType || Nullable.GetUnderlyingType(type) is not null;

    private static DataField CreateScalarField(string name, Type type, bool nullable, MemberInfo? member)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;

        return t switch
        {
            _ when t == typeof(string) || t == typeof(char) || t.IsEnum => new DataField<string>(name, true),
            _ when t == typeof(byte[]) => new DataField<byte[]>(name, true),
            _ when t == typeof(decimal) => DecimalField(name, nullable, member),
            _ when t == typeof(DateTime) || t == typeof(DateTimeOffset) =>
                new DateTimeDataField(name, DateTimeFormat.Timestamp, true, DateTimeTimeUnit.Micros, nullable),
            _ when t == typeof(TimeOnly) => new TimeDataField(name, TimeUnitPrecision.Micros, nullable),
            _ when t == typeof(TimeSpan) => new DataField<long>(name, nullable),
            _ => new DataField(name, t, nullable),
        };
    }

    private static DecimalDataField DecimalField(string name, bool nullable, MemberInfo? member)
    {
        var attribute = member?.GetCustomAttribute<ParquetDecimalAttribute>(true);
        return new DecimalDataField(name, attribute?.Precision ?? DefaultPrecision, attribute?.Scale ?? DefaultScale, isNullable: nullable);
    }
}
