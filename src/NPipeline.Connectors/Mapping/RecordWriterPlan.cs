using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace NPipeline.Connectors.Mapping;

/// <summary>
///     A format's output row, written by column ordinal. A plan compiled by <see cref="RecordWriterPlan" /> calls
///     <see cref="WriteValue{TValue}" /> once per member, with the member's static type, so value types are not boxed.
/// </summary>
/// <remarks>Implement it on a sealed class or a struct: the plan compiles against the concrete type. A <c>null</c> value must be written as the format's null.</remarks>
public interface IFieldWriter
{
    /// <summary>Writes <paramref name="value" /> to the column at <paramref name="ordinal" />.</summary>
    void WriteValue<TValue>(int ordinal, TValue value);
}

/// <summary>The columns and compiled writer for a record type, from <see cref="RecordWriterPlan.Create{T, TWriter}" />.</summary>
/// <typeparam name="T">The record type.</typeparam>
/// <typeparam name="TWriter">The format's writer type.</typeparam>
public sealed class RecordWriterPlan<T, TWriter>
    where TWriter : IFieldWriter
{
    internal RecordWriterPlan(IReadOnlyList<string> columnNames, bool isScalar, Action<TWriter, T> write)
    {
        ColumnNames = columnNames;
        IsScalar = isScalar;
        Write = write;
    }

    /// <summary>The column names, in the order <see cref="Write" /> writes them.</summary>
    public IReadOnlyList<string> ColumnNames { get; }

    /// <summary>Whether <typeparamref name="T" /> is a single value written as one column (a format usually writes no header for it).</summary>
    public bool IsScalar { get; }

    /// <summary>Writes one record's values.</summary>
    public Action<TWriter, T> Write { get; }
}

/// <summary>Builds and caches <see cref="RecordWriterPlan{T, TWriter}" /> instances.</summary>
public static class RecordWriterPlan
{
    private static readonly ConcurrentDictionary<(Type Record, Type Writer, RecordShapeOptions Options), object> Cache = new();

    /// <summary>The plan for writing <typeparamref name="T" /> through <typeparamref name="TWriter" />.</summary>
    public static RecordWriterPlan<T, TWriter> Create<T, TWriter>(RecordShapeOptions? options = null)
        where TWriter : IFieldWriter
    {
        options ??= RecordShapeOptions.Default;
        return (RecordWriterPlan<T, TWriter>)Cache.GetOrAdd((typeof(T), typeof(TWriter), options), static (_, o) => Compile<T, TWriter>(o), options);
    }

    private static RecordWriterPlan<T, TWriter> Compile<T, TWriter>(RecordShapeOptions options)
        where TWriter : IFieldWriter
    {
        var shape = RecordShape.For<T>(options);
        var writer = Expression.Parameter(typeof(TWriter), "writer");
        var item = Expression.Parameter(typeof(T), "item");
        var writeValue = ResolveWriteValue(typeof(TWriter));

        if (shape.IsScalar)
        {
            var scalar = Expression.Call(writer, writeValue.MakeGenericMethod(typeof(T)), Expression.Constant(0), item);
            return new RecordWriterPlan<T, TWriter>([options.Naming.ConvertName("Value")], true, Expression.Lambda<Action<TWriter, T>>(scalar, writer, item).Compile());
        }

        var members = shape.Members.Where(m => m.CanRead).ToList();

        var writes = members.Select((m, ordinal) => (Expression)Expression.Call(
            writer,
            writeValue.MakeGenericMethod(m.Type),
            Expression.Constant(ordinal),
            Expression.MakeMemberAccess(item, m.Member)));

        Expression body = members.Count == 0
            ? Expression.Empty()
            : Expression.Block(writes);

        return new RecordWriterPlan<T, TWriter>(
            [.. members.Select(m => m.ColumnName)],
            false,
            Expression.Lambda<Action<TWriter, T>>(body, writer, item).Compile());
    }

    private static MethodInfo ResolveWriteValue(Type writerType)
    {
        var interfaceMethod = typeof(IFieldWriter).GetMethod(nameof(IFieldWriter.WriteValue))!;

        if (writerType.IsInterface)
            return interfaceMethod;

        var map = writerType.GetInterfaceMap(typeof(IFieldWriter));
        return map.TargetMethods[Array.IndexOf(map.InterfaceMethods, interfaceMethod)];
    }
}
