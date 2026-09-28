using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Errors;

namespace NPipeline.Connectors.Mapping;

/// <summary>
///     A format's current row, read by column ordinal. A record mapper compiled by <see cref="RecordBinder" /> calls
///     <see cref="GetValue{TValue}" /> once per mapped member.
/// </summary>
/// <remarks>
///     Implement it on a sealed class or a struct: the binder compiles against the concrete type, so calls are direct.
///     <see cref="GetValue{TValue}" /> must be strict: return <c>null</c> for a missing value only when
///     the requested type accepts <c>null</c>, and throw (typically <see cref="FieldConversionException" />)
///     for a value that does not convert.
/// </remarks>
public interface IFieldReader
{
    /// <summary>Reads the value at <paramref name="ordinal" /> as <typeparamref name="TValue" />.</summary>
    TValue GetValue<TValue>(int ordinal);
}

/// <summary>What <see cref="RecordBinder" /> does when a mapped member has no column.</summary>
public enum MissingColumnBehavior
{
    /// <summary>
    ///     Fail binding when a <em>required</em> member (C# <c>required</c>, or a constructor parameter without a default)
    ///     or a member with an explicit column name has no column. Other members keep their initialisers.
    /// </summary>
    ThrowForRequired,

    /// <summary>Fail binding when any mapped member has no column.</summary>
    Throw,

    /// <summary>Never fail; members without a column keep their initialisers, and constructor parameters get their defaults.</summary>
    Ignore,
}

/// <summary>Options for <see cref="RecordBinder" />.</summary>
public sealed record RecordBindingOptions
{
    /// <summary>Default shape options and <see cref="MissingColumnBehavior.ThrowForRequired" />.</summary>
    public static RecordBindingOptions Default { get; } = new();

    /// <summary>How members are named and selected.</summary>
    public RecordShapeOptions Shape { get; init; } = RecordShapeOptions.Default;

    /// <summary>What to do when a mapped member has no column.</summary>
    public MissingColumnBehavior MissingColumns { get; init; } = MissingColumnBehavior.ThrowForRequired;
}

/// <summary>
///     Binds a record type to a column layout once per file or schema and compiles the mapper that builds each record.
///     Column names match case-insensitively; with duplicate names, the first column wins.
/// </summary>
public static class RecordBinder
{
    private static readonly ConcurrentDictionary<BindingKey, Delegate> Cache = new();

    /// <summary>
    ///     Returns a mapper that builds a <typeparamref name="T" /> from a <typeparamref name="TReader" /> positioned on a
    ///     row whose columns are <paramref name="columns" />. Mappers are cached by type, options and column layout.
    /// </summary>
    /// <exception cref="RecordBindingException">A column the options require is missing, or the type cannot be constructed.</exception>
    public static Func<TReader, T> Bind<T, TReader>(IReadOnlyList<string> columns, RecordBindingOptions? options = null)
        where TReader : IFieldReader
    {
        ArgumentNullException.ThrowIfNull(columns);
        options ??= RecordBindingOptions.Default;

        var key = new BindingKey(typeof(T), typeof(TReader), options, string.Join('\u001F', columns));
        return (Func<TReader, T>)Cache.GetOrAdd(key, static (_, state) => Compile<T, TReader>(state.Columns, state.Options), (Columns: columns, Options: options));
    }

    private static Func<TReader, T> Compile<T, TReader>(IReadOnlyList<string> columns, RecordBindingOptions options)
        where TReader : IFieldReader
    {
        var shape = RecordShape.For<T>(options.Shape);
        var reader = Expression.Parameter(typeof(TReader), "reader");
        var getValue = ResolveGetValue(typeof(TReader));

        if (shape.IsScalar)
        {
            var column = columns.Count > 0 ? columns[0] : "value";
            var scalar = Guard(Expression.Call(reader, getValue.MakeGenericMethod(typeof(T)), Expression.Constant(0)), typeof(T), [(typeof(T).Name, column)], null);
            return Expression.Lambda<Func<TReader, T>>(scalar, reader).Compile();
        }

        var ordinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < columns.Count; i++)
        {
            _ = ordinals.TryAdd(columns[i], i);
        }

        var readable = shape.Members.Where(m => m.CanWrite || m.ConstructorParameter >= 0).ToList();
        ThrowForMissingColumns(shape, readable, ordinals, columns, options.MissingColumns);

        if (shape.ConstructionError is { } constructionError)
            throw new RecordBindingException(typeof(T), constructionError, [], columns);

        // "field" tracks the member being read, so a failure reports which one without a try/catch per field.
        var field = Expression.Variable(typeof(int), "field");
        var described = new List<(string Member, string Column)>();

        Expression ReadMember(RecordMember member, int ordinal)
        {
            described.Add((member.Name, member.ColumnName));
            var read = Expression.Call(reader, getValue.MakeGenericMethod(member.Type), Expression.Constant(ordinal));
            return Expression.Block(Expression.Assign(field, Expression.Constant(described.Count - 1)), read);
        }

        NewExpression construct;

        if (shape.Constructor is { } constructor && constructor.GetParameters() is { Length: > 0 } parameters)
        {
            var arguments = new Expression[parameters.Length];

            foreach (var member in readable.Where(m => m.ConstructorParameter >= 0))
            {
                var parameter = parameters[member.ConstructorParameter];

                arguments[member.ConstructorParameter] = ordinals.TryGetValue(member.ColumnName, out var ordinal)
                    ? ReadMember(member, ordinal)
                    : DefaultFor(parameter);
            }

            construct = Expression.New(constructor, arguments);
        }
        else
            construct = shape.Constructor is { } parameterless ? Expression.New(parameterless) : Expression.New(typeof(T));

        var bindings = readable
            .Where(m => m.ConstructorParameter < 0 && ordinals.ContainsKey(m.ColumnName))
            .Select(m => (MemberBinding)Expression.Bind(m.Member, ReadMember(m, ordinals[m.ColumnName])))
            .ToList();

        Expression body = bindings.Count > 0
            ? Expression.MemberInit(construct, bindings)
            : construct;

        var guarded = Guard(body, typeof(T), described, field);
        return Expression.Lambda<Func<TReader, T>>(Expression.Block(typeof(T), [field], Expression.Assign(field, Expression.Constant(-1)), guarded), reader).Compile();
    }

    private static MethodInfo ResolveGetValue(Type readerType)
    {
        var interfaceMethod = typeof(IFieldReader).GetMethod(nameof(IFieldReader.GetValue))!;

        if (readerType.IsInterface)
            return interfaceMethod;

        // The concrete implementation, so the compiled call is direct rather than an interface dispatch.
        var map = readerType.GetInterfaceMap(typeof(IFieldReader));
        return map.TargetMethods[Array.IndexOf(map.InterfaceMethods, interfaceMethod)];
    }

    private static void ThrowForMissingColumns(
        RecordShape shape,
        List<RecordMember> readable,
        Dictionary<string, int> ordinals,
        IReadOnlyList<string> columns,
        MissingColumnBehavior behavior)
    {
        if (behavior == MissingColumnBehavior.Ignore)
            return;

        var missing = readable
            .Where(m => !ordinals.ContainsKey(m.ColumnName))
            .Where(m => behavior == MissingColumnBehavior.Throw || m.IsRequired || m.HasExplicitColumn)
            .Select(m => m.ColumnName)
            .ToList();

        if (missing.Count > 0)
            throw new RecordBindingException(shape.Type, null, missing, columns);
    }

    private static Expression DefaultFor(ParameterInfo parameter) =>
        parameter.HasDefaultValue && parameter.DefaultValue is not null
            ? Expression.Convert(Expression.Constant(parameter.DefaultValue), parameter.ParameterType)
            : Expression.Default(parameter.ParameterType);

    private static Expression Guard(Expression body, Type resultType, List<(string Member, string Column)> described, ParameterExpression? field)
    {
        var exception = Expression.Parameter(typeof(Exception), "exception");
        var names = described.ToArray();

        var index = field is null
            ? (Expression)Expression.Constant(0)
            : field;

        var create = Expression.Call(
            typeof(RecordBinder).GetMethod(nameof(CreateFieldException), BindingFlags.NonPublic | BindingFlags.Static)!,
            Expression.Constant(names),
            index,
            exception);

        return Expression.TryCatch(body, Expression.Catch(exception, Expression.Throw(create, resultType)));
    }

    private static FieldMappingException CreateFieldException((string Member, string Column)[] described, int index, Exception exception) =>
        index >= 0 && index < described.Length
            ? new FieldMappingException(described[index].Member, described[index].Column, exception)
            : new FieldMappingException("(constructor)", "(none)", exception);

    private readonly record struct BindingKey(Type RecordType, Type ReaderType, RecordBindingOptions Options, string Columns);
}
