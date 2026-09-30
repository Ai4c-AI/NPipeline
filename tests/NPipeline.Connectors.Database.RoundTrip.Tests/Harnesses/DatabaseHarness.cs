using System.Data.Common;
using NPipeline.Connectors.Database.RoundTrip.Tests.Models;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.Database.RoundTrip.Tests.Harnesses;

public enum WriteMode
{
    PerRow,
    Batch,
    Bulk,
}

/// <summary>Connector-neutral write settings; each harness maps them onto its connector's API.</summary>
public sealed record WriteSettings
{
    public WriteMode Mode { get; init; } = WriteMode.Batch;

    public int BatchSize { get; init; } = 100;

    public bool UseTransaction { get; init; }

    public bool ContinueOnError { get; init; }

    public bool ValidateIdentifiers { get; init; } = true;
}

public sealed record ReadSettings
{
    public bool ContinueOnError { get; init; }

    public bool ThrowOnMappingError { get; init; } = true;
}

/// <summary>
///     One database behind one connector: creates tables with plain SQL, and reads and writes them through the
///     connector's nodes. The tests only talk to this class, so migrating a connector changes its harness, not the tests.
/// </summary>
public abstract class DatabaseHarness
{
    public abstract string Name { get; }

    /// <summary>The connector's default column name for a member (Postgres uses snake_case).</summary>
    public virtual string ColumnName(string member) => member;

    public abstract string Quote(string identifier);

    public abstract string TypeName(ColumnType type);

    public virtual bool Supports(ColumnType type) => true;

    public virtual bool Supports(WriteMode mode) => true;

    public string NewTable() => $"rt_{Guid.NewGuid():N}"[..24];

    public string SelectAll(string table) => $"SELECT * FROM {Quote(table)} ORDER BY {Quote(ColumnName("Id"))}";

    public async Task<string> CreateTableAsync(params Column[] columns)
    {
        var table = NewTable();

        var definitions = columns.Select(c =>
            $"{Quote(c.Explicit ? c.Member : ColumnName(c.Member))} {TypeName(c.Type)}{(c.Nullable ? " NULL" : " NOT NULL")}{(c.Key ? " PRIMARY KEY" : "")}");

        await ExecuteAsync($"CREATE TABLE {Quote(table)} ({string.Join(", ", definitions)})");
        return table;
    }

    public async Task ExecuteAsync(string sql)
    {
        await using var connection = await OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        _ = await command.ExecuteNonQueryAsync();
    }

    public async Task<long> CountAsync(string table)
    {
        await using var connection = await OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {Quote(table)}";
        return Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    public Task WriteAsync<T>(string table, IEnumerable<T> items, WriteSettings? settings = null) =>
        RunSinkAsync(CreateSink<T>(table, settings ?? new WriteSettings()), items);

    public Task<List<T>> ReadAsync<T>(string sql, ReadSettings? settings = null) =>
        RunSourceAsync(CreateSource<T>(sql, settings ?? new ReadSettings()));

    protected abstract Task<DbConnection> OpenAsync();

    protected abstract SinkNode<T> CreateSink<T>(string table, WriteSettings settings);

    public abstract SourceNode<T> CreateSource<T>(string sql, ReadSettings settings);

    /// <summary>The neutral write settings on a SQL connector's sink options.</summary>
    protected static TOptions Apply<TOptions>(TOptions options, WriteSettings settings)
        where TOptions : SqlSinkOptions =>
        options with
        {
            BatchSize = settings.BatchSize,
            Transaction = settings.UseTransaction ? SqlTransactionMode.WholeRun : SqlTransactionMode.PerBatch,
            FailedBatches = settings.ContinueOnError ? FailedBatchAction.DeadLetter : FailedBatchAction.Fail,
            ValidateIdentifiers = settings.ValidateIdentifiers,
        };

    /// <summary>The neutral read settings on a SQL connector's source options: lenient reads skip rows that fail to map.</summary>
    protected static TOptions Apply<TOptions>(TOptions options, ReadSettings settings)
        where TOptions : SqlSourceOptions =>
        settings.ContinueOnError || !settings.ThrowOnMappingError
            ? options with { RowErrorHandler = _ => RowErrorAction.Skip }
            : options;

    public static async Task RunSinkAsync<T>(SinkNode<T> sink, IEnumerable<T> items)
    {
        try
        {
            await using var input = new InMemoryDataStream<T>([.. items]);
            await sink.ConsumeAsync(input, PipelineContext.CreateDefault(), CancellationToken.None);
        }
        finally
        {
            if (sink is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        }
    }

    public static async Task<List<T>> RunSourceAsync<T>(SourceNode<T> source)
    {
        var items = new List<T>();

        try
        {
            await foreach (var item in source.OpenStream(PipelineContext.CreateDefault(), CancellationToken.None))
            {
                items.Add(item);
            }
        }
        finally
        {
            if (source is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        }

        return items;
    }
}
