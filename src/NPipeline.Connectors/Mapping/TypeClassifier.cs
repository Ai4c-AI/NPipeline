namespace NPipeline.Connectors.Mapping;

/// <summary>
///     Decides whether a type is a single value (a scalar) or a record with members. A sink whose item type is scalar
///     writes one column, and a source maps one column into it.
/// </summary>
public static class TypeClassifier
{
    private static readonly HashSet<Type> ScalarTypes =
    [
        typeof(string),
        typeof(bool),
        typeof(char),
        typeof(byte),
        typeof(sbyte),
        typeof(short),
        typeof(ushort),
        typeof(int),
        typeof(uint),
        typeof(long),
        typeof(ulong),
        typeof(float),
        typeof(double),
        typeof(decimal),
        typeof(DateTime),
        typeof(DateTimeOffset),
        typeof(DateOnly),
        typeof(TimeOnly),
        typeof(TimeSpan),
        typeof(Guid),
        typeof(byte[]),
    ];

    /// <summary>
    ///     Returns <c>true</c> for the types the connectors read and write as a single value: primitives, <see cref="string" />,
    ///     <see cref="decimal" />, the date and time types, <see cref="Guid" />, enums, <c>byte[]</c>, and
    ///     <see cref="Nullable{T}" /> of any of them.
    /// </summary>
    public static bool IsScalar(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        return underlying.IsEnum || ScalarTypes.Contains(underlying);
    }
}
