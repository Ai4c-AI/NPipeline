using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace NPipeline.Connectors.Tests.Sql;

/// <summary>A statement a fake connection ran, with its parameter values in order.</summary>
public sealed record FakeStatement(string Sql, IReadOnlyList<object?> Values, IReadOnlyList<string> Names);

/// <summary>
///     An in-memory ADO.NET provider for testing the SQL bases: it records every statement, keeps statements run inside
///     a transaction until the transaction commits, returns <see cref="QueryResult" /> to queries, and fails executions
///     that <see cref="Fail" /> picks.
/// </summary>
public sealed class FakeDatabase
{
    /// <summary>Every statement executed, committed or not.</summary>
    public List<FakeStatement> Executed { get; } = [];

    /// <summary>Statements that took effect: run outside a transaction, or in one that committed.</summary>
    public List<FakeStatement> Committed { get; } = [];

    /// <summary>Transactions begun, committed and rolled back.</summary>
    public int Begun { get; set; }

    public int Commits { get; set; }

    public int Rollbacks { get; set; }

    public int Opens { get; set; }

    /// <summary>Decides, from the 1-based execution number and the statement, whether that execution throws.</summary>
    public Func<int, FakeStatement, Exception?> Fail { get; set; } = (_, _) => null;

    /// <summary>The rows a query returns.</summary>
    public DataTable QueryResult { get; set; } = new();

    public FakeConnection Connect() => new(this);

    internal void Record(FakeStatement statement, FakeTransaction? transaction)
    {
        Executed.Add(statement);

        if (Fail(Executed.Count, statement) is { } error)
            throw error;

        if (transaction is null)
            Committed.Add(statement);
        else
            transaction.Pending.Add(statement);
    }
}

/// <summary>The exception a fake statement throws for a transient failure.</summary>
public sealed class TransientFakeException(string message) : Exception(message);

[SuppressMessage("Design", "CA1010", Justification = "Test double.")]
public sealed class FakeConnection(FakeDatabase database) : DbConnection
{
    private ConnectionState _state = ConnectionState.Closed;

    public FakeDatabase Store { get; } = database;

    [AllowNull]
    public override string ConnectionString { get; set; } = "fake";

    public override string DataSource => "fake";

    public override string ServerVersion => "1";

    public override ConnectionState State => _state;

    public override string Database => "fake";

    public override void ChangeDatabase(string databaseName)
    {
    }

    public override void Close() => _state = ConnectionState.Closed;

    public override void Open()
    {
        Store.Opens++;
        _state = ConnectionState.Open;
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        Store.Begun++;
        return new FakeTransaction(this);
    }

    protected override DbCommand CreateDbCommand() => new FakeCommand(this);
}

public sealed class FakeTransaction(FakeConnection connection) : DbTransaction
{
    internal List<FakeStatement> Pending { get; } = [];

    public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

    protected override DbConnection DbConnection => connection;

    public override void Commit()
    {
        connection.Store.Committed.AddRange(Pending);
        connection.Store.Commits++;
        Pending.Clear();
    }

    public override void Rollback()
    {
        connection.Store.Rollbacks++;
        Pending.Clear();
    }
}

[SuppressMessage("Security", "CA2100", Justification = "Test double.")]
public sealed class FakeCommand(FakeConnection connection) : DbCommand
{
    private readonly FakeParameterCollection _parameters = new();

    [AllowNull]
    public override string CommandText { get; set; } = string.Empty;

    public override int CommandTimeout { get; set; }

    public override CommandType CommandType { get; set; } = CommandType.Text;

    public override bool DesignTimeVisible { get; set; }

    public override UpdateRowSource UpdatedRowSource { get; set; }

    protected override DbConnection? DbConnection { get; set; } = connection;

    protected override DbParameterCollection DbParameterCollection => _parameters;

    protected override DbTransaction? DbTransaction { get; set; }

    public override void Cancel()
    {
    }

    public override int ExecuteNonQuery()
    {
        connection.Store.Record(Snapshot(), (FakeTransaction?)DbTransaction);
        return 1;
    }

    public override object? ExecuteScalar() => ExecuteNonQuery();

    public override void Prepare()
    {
    }

    protected override DbParameter CreateDbParameter() => new FakeParameter();

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        connection.Store.Record(Snapshot(), (FakeTransaction?)DbTransaction);
        return connection.Store.QueryResult.CreateDataReader();
    }

    private FakeStatement Snapshot() =>
        new(CommandText, [.. _parameters.Items.Select(p => p.Value is DBNull ? null : p.Value)], [.. _parameters.Items.Select(p => p.ParameterName)]);
}

public sealed class FakeParameter : DbParameter
{
    public override DbType DbType { get; set; }

    public override ParameterDirection Direction { get; set; }

    public override bool IsNullable { get; set; }

    [AllowNull]
    public override string ParameterName { get; set; } = string.Empty;

    public override int Size { get; set; }

    [AllowNull]
    public override string SourceColumn { get; set; } = string.Empty;

    public override bool SourceColumnNullMapping { get; set; }

    public override object? Value { get; set; }

    public override void ResetDbType() => DbType = DbType.Object;
}

[SuppressMessage("Design", "CA1010", Justification = "Test double.")]
public sealed class FakeParameterCollection : DbParameterCollection
{
    public List<DbParameter> Items { get; } = [];

    public override int Count => Items.Count;

    public override object SyncRoot => Items;

    public override int Add(object value)
    {
        Items.Add((DbParameter)value);
        return Items.Count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (var value in values)
        {
            _ = Add(value!);
        }
    }

    public override void Clear() => Items.Clear();

    public override bool Contains(object value) => Items.Contains((DbParameter)value);

    public override bool Contains(string value) => Items.Any(p => p.ParameterName == value);

    public override void CopyTo(Array array, int index) => ((ICollection)Items).CopyTo(array, index);

    public override IEnumerator GetEnumerator() => Items.GetEnumerator();

    public override int IndexOf(object value) => Items.IndexOf((DbParameter)value);

    public override int IndexOf(string parameterName) => Items.FindIndex(p => p.ParameterName == parameterName);

    public override void Insert(int index, object value) => Items.Insert(index, (DbParameter)value);

    public override void Remove(object value) => Items.Remove((DbParameter)value);

    public override void RemoveAt(int index) => Items.RemoveAt(index);

    public override void RemoveAt(string parameterName) => Items.RemoveAt(IndexOf(parameterName));

    protected override DbParameter GetParameter(int index) => Items[index];

    protected override DbParameter GetParameter(string parameterName) => Items[IndexOf(parameterName)];

    protected override void SetParameter(int index, DbParameter value) => Items[index] = value;

    protected override void SetParameter(string parameterName, DbParameter value) => Items[IndexOf(parameterName)] = value;
}
