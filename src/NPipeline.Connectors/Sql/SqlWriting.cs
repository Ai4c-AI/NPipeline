using System.Collections;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Sql;

/// <summary>The type a member's values are written as: an enum's underlying type, <c>string</c> for <c>char</c>, otherwise the member type without <c>Nullable</c>.</summary>
internal static class SqlStorage
{
    public static Type TypeOf(Type memberType)
    {
        var underlying = Nullable.GetUnderlyingType(memberType) ?? memberType;

        if (underlying.IsEnum)
            return Enum.GetUnderlyingType(underlying);

        return underlying == typeof(char) ? typeof(string) : underlying;
    }
}

/// <summary>A member value as a provider takes it: enums as their underlying integer, <c>char</c> as text, everything else as is.</summary>
internal static class SqlValue<TValue>
{
    public static readonly Func<TValue, object?> Box = Create();

    private static Func<TValue, object?> Create()
    {
        var underlying = Nullable.GetUnderlyingType(typeof(TValue)) ?? typeof(TValue);

        if (!underlying.IsEnum && underlying != typeof(char))
            return static value => value;

        var parameter = Expression.Parameter(typeof(TValue), "value");
        var storage = SqlStorage.TypeOf(typeof(TValue));
        Expression plain = Nullable.GetUnderlyingType(typeof(TValue)) is null ? parameter : Expression.Property(parameter, "Value");

        Expression converted = underlying == typeof(char)
            ? Expression.Call(plain, typeof(char).GetMethod(nameof(ToString), Type.EmptyTypes)!)
            : Expression.Convert(plain, storage);

        Expression body = Expression.Convert(converted, typeof(object));

        if (Nullable.GetUnderlyingType(typeof(TValue)) is not null)
            body = Expression.Condition(Expression.Property(parameter, "HasValue"), body, Expression.Constant(null, typeof(object)));

        return Expression.Lambda<Func<TValue, object?>>(body, parameter).Compile();
    }
}

/// <summary>A column a SQL sink writes.</summary>
/// <param name="Name">The column name.</param>
/// <param name="StorageType">The type of its values: an enum's underlying type, <c>string</c> for <c>char</c>, otherwise the member type without <c>Nullable</c>.</param>
/// <param name="Member">The member it comes from, for dialects that read connector attributes; <c>null</c> for a scalar record.</param>
/// <param name="IsNullable">Whether the member can hold <c>null</c> (a reference type or <c>Nullable</c>).</param>
public sealed record SqlColumn(string Name, Type StorageType, MemberInfo? Member, bool IsNullable);

/// <summary>Collects one record's values, in column order, as the compiled plan writes them. A struct, so extraction allocates nothing.</summary>
internal readonly struct SqlValueRow(object?[] values) : IFieldWriter
{
    public void WriteValue<TValue>(int ordinal, TValue value) => values[ordinal] = SqlValue<TValue>.Box(value);
}

/// <summary>
///     What a SQL sink writes for <typeparamref name="T" />: the columns (after the naming policy), their storage types,
///     and a compiled plan that extracts a record's values. Built once per type and shape.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public sealed class SqlWritePlan<T>
{
    private static readonly ConcurrentDictionary<RecordShapeOptions, SqlWritePlan<T>> Cache = new();

    private readonly RecordWriterPlan<T, SqlValueRow> _plan;

    private SqlWritePlan(RecordShapeOptions options)
    {
        var shape = RecordShape.For<T>(options);
        _plan = RecordWriterPlan.Create<T, SqlValueRow>(options);
        ColumnNames = _plan.ColumnNames;

        Columns = _plan.IsScalar
            ? [new SqlColumn(ColumnNames[0], SqlStorage.TypeOf(typeof(T)), null, IsNullable(typeof(T)))]
            : [.. shape.Members.Where(m => m.CanRead).Select((m, i) => new SqlColumn(ColumnNames[i], SqlStorage.TypeOf(m.Type), m.Member, IsNullable(m.Type)))];

        if (Columns.Count != ColumnNames.Count)
            throw new InvalidOperationException($"The write plan for {typeof(T).Name} has {ColumnNames.Count} columns but {Columns.Count} members.");
    }

    /// <summary>The column names, in the order <see cref="Extract" /> writes values.</summary>
    public IReadOnlyList<string> ColumnNames { get; }

    private static bool IsNullable(Type type) => !type.IsValueType || Nullable.GetUnderlyingType(type) is not null;

    /// <summary>The columns, in the order <see cref="Extract" /> writes values.</summary>
    public IReadOnlyList<SqlColumn> Columns { get; }

    /// <summary>The plan for <typeparamref name="T" /> under <paramref name="options" />, built once.</summary>
#pragma warning disable CA1000 // The plan is per type, so its cache is too.
    public static SqlWritePlan<T> For(RecordShapeOptions options) => Cache.GetOrAdd(options, static o => new SqlWritePlan<T>(o));
#pragma warning restore CA1000

    /// <summary>Writes <paramref name="item" />'s values into <paramref name="values" />, which must have one slot per column.</summary>
    public void Extract(T item, object?[] values) => _plan.Write(new SqlValueRow(values), item);
}

/// <summary>Everything a writer needs to write <typeparamref name="T" /> to one table.</summary>
/// <typeparam name="T">The record type.</typeparam>
public sealed class SqlWriteTarget<T>
{
    internal SqlWriteTarget(SqlDialect dialect, SqlSinkOptions options, SqlWritePlan<T> plan)
    {
        Dialect = dialect;
        Plan = plan;
        Table = options.Table;
        QualifiedTable = dialect.QualifiedTable(options.Schema, options.Table);
        QuotedColumns = [.. plan.ColumnNames.Select(dialect.QuoteIdentifier)];
        CommandTimeout = options.CommandTimeout;
        Upsert = options.Upsert;

        if (options.Upsert is { } upsert)
        {
            var unknown = upsert.Keys.Where(k => !plan.ColumnNames.Contains(k, StringComparer.OrdinalIgnoreCase)).ToList();

            if (unknown.Count > 0)
            {
                throw new ArgumentException(
                    $"Upsert key {string.Join(", ", unknown)} is not a column {typeof(T).Name} writes. Columns: {string.Join(", ", plan.ColumnNames)}.",
                    nameof(options));
            }

            QuotedKeys = [.. upsert.Keys.Select(dialect.QuoteIdentifier)];
        }
    }

    /// <summary>The dialect.</summary>
    public SqlDialect Dialect { get; }

    /// <summary>The write plan.</summary>
    public SqlWritePlan<T> Plan { get; }

    /// <summary>The table name as configured.</summary>
    public string Table { get; }

    /// <summary>The quoted, schema-qualified table.</summary>
    public string QualifiedTable { get; }

    /// <summary>The quoted column names.</summary>
    public IReadOnlyList<string> QuotedColumns { get; }

    /// <summary>The quoted upsert keys, when <see cref="Upsert" /> is set.</summary>
    public IReadOnlyList<string> QuotedKeys { get; } = [];

    /// <summary>The upsert, if any.</summary>
    public SqlUpsert? Upsert { get; }

    /// <summary>The command timeout in seconds.</summary>
    public int CommandTimeout { get; }

    /// <summary>The <c>INSERT</c> or upsert statement for <paramref name="rows" /> rows.</summary>
    public string Statement(int rows) =>
        Upsert is { } upsert
            ? Dialect.Upsert(QualifiedTable, QuotedColumns, QuotedKeys, upsert.OnMatch, rows)
            : Dialect.Insert(QualifiedTable, QuotedColumns, rows);
}

/// <summary>Writes batches of records to a table. The SQL sink calls it once per batch, inside the batch's transaction if any.</summary>
/// <typeparam name="T">The record type.</typeparam>
public abstract class SqlWriter<T>
{
    /// <summary>Creates a writer for <paramref name="target" />.</summary>
    protected SqlWriter(SqlWriteTarget<T> target) => Target = target;

    /// <summary>The table and plan written.</summary>
    protected SqlWriteTarget<T> Target { get; }

    /// <summary>
    ///     Each column's database type name, in plan order, when the dialect <see cref="SqlDialect.NeedsColumnTypes" />;
    ///     otherwise <c>null</c> for every column. Set by the sink before the first batch.
    /// </summary>
    public IReadOnlyList<string?> DatabaseTypes { get; internal set; } = [];

    /// <summary>The database type of column <paramref name="index" />, or <c>null</c>.</summary>
    protected string? DatabaseType(int index) => index < DatabaseTypes.Count ? DatabaseTypes[index] : null;

    /// <summary>Writes <paramref name="batch" />. It either all lands or the call throws, unless the database cannot do that without a transaction.</summary>
    public abstract Task WriteAsync(DbConnection connection, DbTransaction? transaction, IReadOnlyList<T> batch, CancellationToken cancellationToken);
}

/// <summary>Writes each batch as multi-row <c>INSERT</c> (or upsert) statements, split to stay under the parameter limit.</summary>
/// <typeparam name="T">The record type.</typeparam>
public sealed class SqlBatchWriter<T> : SqlWriter<T>
{
    private readonly int _rowsPerStatement;
    private string? _fullStatement;

    /// <summary>Creates the writer.</summary>
    public SqlBatchWriter(SqlWriteTarget<T> target)
        : base(target)
    {
        _rowsPerStatement = Math.Clamp(target.Dialect.MaxParameters / Math.Max(1, target.QuotedColumns.Count), 1, Math.Max(1, target.Dialect.MaxRowsPerStatement));
    }

    /// <inheritdoc />
    public override async Task WriteAsync(DbConnection connection, DbTransaction? transaction, IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        var columns = Target.QuotedColumns.Count;
        var types = Target.Plan.Columns;
        var values = new object?[columns];

        for (var start = 0; start < batch.Count; start += _rowsPerStatement)
        {
            var rows = Math.Min(_rowsPerStatement, batch.Count - start);

            var command = connection.CreateCommand();
            await using var commandScope = command.ConfigureAwait(false);
            command.Transaction = transaction;
            command.CommandTimeout = Target.CommandTimeout;
            command.CommandText = rows == _rowsPerStatement ? _fullStatement ??= Target.Statement(rows) : Target.Statement(rows);

            for (var row = 0; row < rows; row++)
            {
                Target.Plan.Extract(batch[start + row], values);

                for (var column = 0; column < columns; column++)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = Target.Dialect.ParameterName((row * columns) + column);
                    Target.Dialect.Bind(parameter, types[column], DatabaseType(column), values[column]);
                    _ = command.Parameters.Add(parameter);
                }
            }

            _ = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

/// <summary>Writes one row per statement, reusing one command and its parameters.</summary>
/// <typeparam name="T">The record type.</typeparam>
public sealed class SqlPerRowWriter<T> : SqlWriter<T>
{
    /// <summary>Creates the writer.</summary>
    public SqlPerRowWriter(SqlWriteTarget<T> target)
        : base(target)
    {
    }

    /// <inheritdoc />
    public override async Task WriteAsync(DbConnection connection, DbTransaction? transaction, IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        var columns = Target.QuotedColumns.Count;
        var types = Target.Plan.Columns;
        var values = new object?[columns];

        var command = connection.CreateCommand();
        await using var commandScope = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandTimeout = Target.CommandTimeout;
        command.CommandText = Target.Statement(1);

        var parameters = new DbParameter[columns];

        for (var column = 0; column < columns; column++)
        {
            parameters[column] = command.CreateParameter();
            parameters[column].ParameterName = Target.Dialect.ParameterName(column);
            _ = command.Parameters.Add(parameters[column]);
        }

        foreach (var item in batch)
        {
            Target.Plan.Extract(item, values);

            for (var column = 0; column < columns; column++)
            {
                Target.Dialect.Bind(parameters[column], types[column], DatabaseType(column), values[column]);
            }

            _ = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

/// <summary>
///     A forward-only <see cref="DbDataReader" /> over a batch's extracted values, for bulk APIs that read rows from a data
///     reader (<c>SqlBulkCopy</c>, <c>MySqlBulkCopy</c>). Values are extracted as each row is read, so no table is built.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
#pragma warning disable CA1010 // DbDataReader is IEnumerable by design; the bulk APIs read it as a data reader.
public sealed class SqlBatchDataReader<T> : DbDataReader
{
    private readonly IReadOnlyList<T> _batch;
    private readonly SqlWriteTarget<T> _target;
    private readonly object?[] _values;
    private int _row = -1;

    /// <summary>Creates a reader over <paramref name="batch" />.</summary>
    public SqlBatchDataReader(SqlWriteTarget<T> target, IReadOnlyList<T> batch)
    {
        _target = target;
        _batch = batch;
        _values = new object?[target.Plan.ColumnNames.Count];
    }

    /// <inheritdoc />
    public override int FieldCount => _values.Length;

    /// <inheritdoc />
    public override bool HasRows => _batch.Count > 0;

    /// <inheritdoc />
    public override bool IsClosed => false;

    /// <inheritdoc />
    public override int RecordsAffected => -1;

    /// <inheritdoc />
    public override int Depth => 0;

    /// <inheritdoc />
    public override object this[int ordinal] => GetValue(ordinal);

    /// <inheritdoc />
    public override object this[string name] => GetValue(GetOrdinal(name));

    /// <inheritdoc />
    public override bool Read()
    {
        if (++_row >= _batch.Count)
            return false;

        _target.Plan.Extract(_batch[_row], _values);
        return true;
    }

    /// <inheritdoc />
    public override bool NextResult() => false;

    /// <inheritdoc />
    public override string GetName(int ordinal) => _target.Plan.ColumnNames[ordinal];

    /// <inheritdoc />
    public override int GetOrdinal(string name)
    {
        for (var i = 0; i < _values.Length; i++)
        {
            if (string.Equals(_target.Plan.ColumnNames[i], name, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        throw new IndexOutOfRangeException(name);
    }

    /// <inheritdoc />
    public override Type GetFieldType(int ordinal) => _target.Plan.Columns[ordinal].StorageType;

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;

    /// <inheritdoc />
    public override object GetValue(int ordinal) => _values[ordinal] ?? DBNull.Value;

    /// <inheritdoc />
    public override int GetValues(object[] values)
    {
        var count = Math.Min(values.Length, _values.Length);

        for (var i = 0; i < count; i++)
        {
            values[i] = GetValue(i);
        }

        return count;
    }

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal) => _values[ordinal] is null;

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal) => (bool)_values[ordinal]!;

    /// <inheritdoc />
    public override byte GetByte(int ordinal) => (byte)_values[ordinal]!;

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        var bytes = (byte[])_values[ordinal]!;

        if (buffer is null)
            return bytes.Length;

        var count = (int)Math.Min(length, bytes.Length - dataOffset);
        Array.Copy(bytes, dataOffset, buffer, bufferOffset, count);
        return count;
    }

    /// <inheritdoc />
    public override char GetChar(int ordinal) => ((string)_values[ordinal]!)[0];

    /// <inheritdoc />
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var text = (string)_values[ordinal]!;

        if (buffer is null)
            return text.Length;

        var count = (int)Math.Min(length, text.Length - dataOffset);
        text.CopyTo((int)dataOffset, buffer, bufferOffset, count);
        return count;
    }

    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal) => (DateTime)_values[ordinal]!;

    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal) => (decimal)_values[ordinal]!;

    /// <inheritdoc />
    public override double GetDouble(int ordinal) => (double)_values[ordinal]!;

    /// <inheritdoc />
    public override float GetFloat(int ordinal) => (float)_values[ordinal]!;

    /// <inheritdoc />
    public override Guid GetGuid(int ordinal) => (Guid)_values[ordinal]!;

    /// <inheritdoc />
    public override short GetInt16(int ordinal) => (short)_values[ordinal]!;

    /// <inheritdoc />
    public override int GetInt32(int ordinal) => (int)_values[ordinal]!;

    /// <inheritdoc />
    public override long GetInt64(int ordinal) => (long)_values[ordinal]!;

    /// <inheritdoc />
    public override string GetString(int ordinal) => (string)_values[ordinal]!;

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => new DbEnumerator(this);
}
#pragma warning restore CA1010
