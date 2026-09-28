using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Mapping;

/// <summary>How a <see cref="RecordShape" /> names and selects members. Connectors pass their own attributes through these hooks.</summary>
public sealed record RecordShapeOptions
{
    /// <summary>As-is naming, <see cref="ColumnAttribute" /> and <see cref="IgnoreColumnAttribute" /> only.</summary>
    public static RecordShapeOptions Default { get; } = new();

    /// <summary>The policy for members without an explicit column name. Defaults to <see cref="ColumnNamingPolicy.AsIs" />.</summary>
    public ColumnNamingPolicy Naming { get; init; } = ColumnNamingPolicy.AsIs;

    /// <summary>
    ///     An explicit column name from a connector's own attribute (for example <c>[JsonPropertyName]</c>), or <c>null</c>
    ///     to fall through to the naming policy. <see cref="ColumnAttribute" /> is checked first.
    /// </summary>
    public Func<MemberInfo, string?>? ColumnName { get; init; }

    /// <summary>Excludes members a connector's own attribute ignores (for example <c>[JsonIgnore]</c>), in addition to <see cref="IgnoreColumnAttribute" />.</summary>
    public Func<MemberInfo, bool>? IsIgnored { get; init; }
}

/// <summary>One mapped member of a <see cref="RecordShape" />.</summary>
public sealed class RecordMember
{
    internal RecordMember(MemberInfo member, Type type, string columnName, bool hasExplicitColumn, bool canRead, bool canWrite, bool isRequired)
    {
        Member = member;
        Type = type;
        ColumnName = columnName;
        HasExplicitColumn = hasExplicitColumn;
        CanRead = canRead;
        CanWrite = canWrite;
        IsRequired = isRequired;
    }

    /// <summary>The property or field.</summary>
    public MemberInfo Member { get; }

    /// <summary>The CLR member name.</summary>
    public string Name => Member.Name;

    /// <summary>The member's type.</summary>
    public Type Type { get; }

    /// <summary>The column the member reads from and writes to.</summary>
    public string ColumnName { get; }

    /// <summary>Whether the column name came from an attribute rather than the naming policy.</summary>
    public bool HasExplicitColumn { get; }

    /// <summary>Whether the member has a public getter, so a sink can write it.</summary>
    public bool CanRead { get; }

    /// <summary>Whether the member has a public setter or <c>init</c> accessor.</summary>
    public bool CanWrite { get; }

    /// <summary>Whether the member is C# <c>required</c>, or is set by a constructor parameter without a default value.</summary>
    public bool IsRequired { get; internal set; }

    /// <summary>The index of the constructor parameter that sets this member, or -1 when a setter does.</summary>
    public int ConstructorParameter { get; internal set; } = -1;
}

/// <summary>
///     The mapped members of a record type and how to construct it, reflected once per type and options and cached.
///     Shared by every connector, so they agree on naming, ignored members and construction.
/// </summary>
public sealed class RecordShape
{
    private static readonly ConcurrentDictionary<(Type Type, RecordShapeOptions Options), RecordShape> Cache = new();

    private RecordShape(Type type, IReadOnlyList<RecordMember> members, ConstructorInfo? constructor, string? constructionError)
    {
        Type = type;
        Members = members;
        Constructor = constructor;
        ConstructionError = constructionError;
    }

    /// <summary>The record type.</summary>
    public Type Type { get; }

    /// <summary>Whether the type is a single value (<see cref="TypeClassifier.IsScalar" />); a scalar shape has no members.</summary>
    public bool IsScalar => TypeClassifier.IsScalar(Type);

    /// <summary>The mapped members, base-class members first, each class's members in declaration order.</summary>
    public IReadOnlyList<RecordMember> Members { get; }

    /// <summary>
    ///     The constructor that creates instances: a parameterless one, or the one whose parameters all match members.
    ///     <c>null</c> for a value type (its default constructor) or when <see cref="ConstructionError" /> is set.
    /// </summary>
    public ConstructorInfo? Constructor { get; }

    /// <summary>Why the type cannot be constructed from columns, or <c>null</c> when it can. Only reading needs construction.</summary>
    public string? ConstructionError { get; }

    /// <summary>The shape of <typeparamref name="T" />.</summary>
    public static RecordShape For<T>(RecordShapeOptions? options = null) => For(typeof(T), options);

    /// <summary>The shape of <paramref name="type" />.</summary>
    public static RecordShape For(Type type, RecordShapeOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(type);
        return Cache.GetOrAdd((type, options ?? RecordShapeOptions.Default), static key => Build(key.Type, key.Options));
    }

    private static RecordShape Build(Type type, RecordShapeOptions options)
    {
        if (TypeClassifier.IsScalar(type))
            return new RecordShape(type, [], null, null);

        var members = DiscoverMembers(type, options);
        var (constructor, error) = SelectConstructor(type, members);
        return new RecordShape(type, members, constructor, error);
    }

    private static List<RecordMember> DiscoverMembers(Type type, RecordShapeOptions options)
    {
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.GetIndexParameters().Length == 0)
            .Select(p => (Member: (MemberInfo)p, p.PropertyType, CanRead: p.GetMethod?.IsPublic == true, CanWrite: p.SetMethod?.IsPublic == true));

        // Public fields are mapped only when they opt in with [Column], so a type's incidental fields stay out.
        var fields = type.GetFields(BindingFlags.Public | BindingFlags.Instance)
            .Where(f => f.IsDefined(typeof(ColumnAttribute), true))
            .Select(f => (Member: (MemberInfo)f, f.FieldType, CanRead: true, CanWrite: !f.IsInitOnly));

        return
        [
            .. properties.Concat(fields)
                .Where(m => !IsIgnored(m.Member, options))
                .OrderBy(m => InheritanceDepth(m.Member.DeclaringType!))
                .ThenBy(m => m.Member.MetadataToken)
                .Select(m =>
                {
                    var (columnName, isExplicit) = ResolveColumnName(m.Member, options);
                    var isRequired = m.Member.IsDefined(typeof(RequiredMemberAttribute), true);
                    return new RecordMember(m.Member, m.Item2, columnName, isExplicit, m.CanRead, m.CanWrite, isRequired);
                }),
        ];
    }

    private static bool IsIgnored(MemberInfo member, RecordShapeOptions options) =>
        member.IsDefined(typeof(IgnoreColumnAttribute), true)
        || member.GetCustomAttribute<ColumnAttribute>(true)?.Ignore == true
        || options.IsIgnored?.Invoke(member) == true;

    private static (string Name, bool IsExplicit) ResolveColumnName(MemberInfo member, RecordShapeOptions options)
    {
        if (member.GetCustomAttribute<ColumnAttribute>(true) is { Name: { Length: > 0 } attributeName })
            return (attributeName, true);

        if (options.ColumnName?.Invoke(member) is { Length: > 0 } connectorName)
            return (connectorName, true);

        return (options.Naming.ConvertName(member.Name), false);
    }

    private static int InheritanceDepth(Type type)
    {
        var depth = 0;

        for (var current = type.BaseType; current is not null; current = current.BaseType)
        {
            depth++;
        }

        return depth;
    }

    private static (ConstructorInfo? Constructor, string? Error) SelectConstructor(Type type, List<RecordMember> members)
    {
        if (type.IsAbstract || type.IsInterface)
            return (null, $"'{type.FullName}' is abstract and cannot be constructed.");

        if (type.IsValueType)
            return (null, null);

        var constructors = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance);

        if (constructors.FirstOrDefault(c => c.GetParameters().Length == 0) is { } parameterless)
            return (parameterless, null);

        // Positional records and primary constructors: every parameter must set a member of the same name.
        var byName = members.ToDictionary(m => m.Name, StringComparer.OrdinalIgnoreCase);

        var match = constructors
            .Select(c => (Constructor: c, Parameters: c.GetParameters()))
            .Where(c => c.Parameters.All(p => p.Name is not null && byName.TryGetValue(p.Name, out var m) && m.Type == p.ParameterType))
            .OrderByDescending(c => c.Parameters.Length)
            .FirstOrDefault();

        if (match.Constructor is null)
        {
            return (null,
                $"'{type.FullName}' has no parameterless constructor and no public constructor whose parameters all match its members by name and type.");
        }

        for (var i = 0; i < match.Parameters.Length; i++)
        {
            var member = byName[match.Parameters[i].Name!];
            member.ConstructorParameter = i;
            member.IsRequired |= !match.Parameters[i].HasDefaultValue;
        }

        return (match.Constructor, null);
    }
}
