using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;
using Parquet;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet.Tests;

/// <summary>An in-memory store and helpers to run Parquet nodes against it.</summary>
public abstract class ParquetTestBase
{
    protected InMemoryStorageProvider Provider { get; } = new();

    protected static StorageUri Uri(string path = "data.parquet") => InMemoryStorageProvider.Uri(path);

    protected ParquetSourceNode<T> Source<T>(Func<ParquetReadOptions, ParquetReadOptions>? configure = null, string path = "data.parquet") =>
        ParquetConnector.Source<T>(Uri(path), o => (configure ?? (x => x))(o with { Provider = Provider }));

    protected ParquetSourceNode<T> Source<T>(Func<ParquetRow, T> map, Func<ParquetReadOptions, ParquetReadOptions>? configure = null, string path = "data.parquet") =>
        ParquetConnector.Source(Uri(path), map, o => (configure ?? (x => x))(o with { Provider = Provider }));

    protected ParquetSinkNode<T> Sink<T>(Func<ParquetWriteOptions, ParquetWriteOptions>? configure = null, string path = "data.parquet") =>
        ParquetConnector.Sink<T>(Uri(path), o => (configure ?? (x => x))(o with { Provider = Provider }));

    protected static async Task<List<T>> ReadAsync<T>(SourceNode<T> source)
    {
        var rows = new List<T>();

        await foreach (var row in source.OpenStream(new PipelineContext(), CancellationToken.None))
        {
            rows.Add(row);
        }

        return rows;
    }

    protected static async Task WriteAsync<T>(SinkNode<T> sink, IEnumerable<T> items)
    {
        await using var input = new DataStream<T>(Enumerate(items), "items");
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
    }

    /// <summary>Writes <paramref name="items" /> to <paramref name="path" /> with default options.</summary>
    protected Task PutAsync<T>(IEnumerable<T> items, string path = "data.parquet", Func<ParquetWriteOptions, ParquetWriteOptions>? configure = null) =>
        WriteAsync(Sink<T>(configure, path), items);

    /// <summary>Writes a file with Parquet.Net directly, for layouts this connector does not write.</summary>
    protected async Task PutRawAsync(string path, ParquetSchema schema, Func<ParquetRowGroupWriter, Task> writeRowGroup)
    {
        using var stream = new MemoryStream();

        await using (var writer = await ParquetWriter.CreateAsync(schema, stream))
        {
            using var rowGroup = writer.CreateRowGroup();
            await writeRowGroup(rowGroup);
        }

        Provider.Put(Uri(path), stream.ToArray());
    }

    /// <summary>The file's schema and the row count of each row group.</summary>
    protected async Task<(ParquetSchema Schema, long[] RowGroups, CompressionMethod Codec)> InspectAsync(string path = "data.parquet")
    {
        using var stream = new MemoryStream(Provider.Get(Uri(path)));
        await using var reader = await ParquetReader.CreateAsync(stream);
        var groups = new long[reader.RowGroupCount];

        for (var i = 0; i < groups.Length; i++)
        {
            using var group = reader.OpenRowGroupReader(i);
            groups[i] = group.RowCount;
        }

        var codec = reader.Metadata!.RowGroups.Count == 0 ? CompressionMethod.None : (CompressionMethod)(int)reader.Metadata.RowGroups[0].Columns[0].MetaData!.Codec;
        return (reader.Schema, groups, codec);
    }

    private static async IAsyncEnumerable<T> Enumerate<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }
}

public enum Tier
{
    Bronze,
    Silver,
    Gold,
}

public sealed record Order
{
    public int Id { get; init; }

    public string Customer { get; init; } = string.Empty;

    [ParquetDecimal(18, 2)]
    public decimal Amount { get; init; }

    public string Region { get; init; } = "AU";

    public static Order Create(int i) => new() { Id = i, Customer = $"c{i}", Amount = i * 1.25m, Region = i % 2 == 0 ? "EU" : "US" };
}

public sealed record PositionalOrder(int Id, string Customer);

/// <summary>Every scalar type the connector maps, in required and nullable forms.</summary>
#pragma warning disable CA1720 // Members are named after the types they cover.
public sealed record AllTypes
{
    public bool Flag { get; init; }
    public byte Byte { get; init; }
    public sbyte SByte { get; init; }
    public short Short { get; init; }
    public ushort UShort { get; init; }
    public int Int { get; init; }
    public uint UInt { get; init; }
    public long Long { get; init; }
    public ulong ULong { get; init; }
    public float Float { get; init; }
    public double Double { get; init; }
    public decimal Decimal { get; init; }

    [ParquetDecimal(10, 3)]
    public decimal Money { get; init; }

    public string Text { get; init; } = string.Empty;
    public char Char { get; init; }
    public byte[] Bytes { get; init; } = [];
    public DateTime At { get; init; }
    public DateTimeOffset Offset { get; init; }
    public DateOnly Day { get; init; }
    public TimeOnly Time { get; init; }
    public TimeSpan Span { get; init; }
    public Guid Key { get; init; }
    public Tier Tier { get; init; }

    public int? MaybeInt { get; init; }
    public double? MaybeDouble { get; init; }
    public decimal? MaybeDecimal { get; init; }
    public string? MaybeText { get; init; }
    public byte[]? MaybeBytes { get; init; }
    public DateTime? MaybeAt { get; init; }
    public DateTimeOffset? MaybeOffset { get; init; }
    public DateOnly? MaybeDay { get; init; }
    public TimeOnly? MaybeTime { get; init; }
    public TimeSpan? MaybeSpan { get; init; }
    public Guid? MaybeKey { get; init; }
    public Tier? MaybeTier { get; init; }

    public static AllTypes Create(int i) => new()
    {
        Flag = i % 2 == 0,
        Byte = (byte)i,
        SByte = (sbyte)-i,
        Short = (short)(-1000 - i),
        UShort = (ushort)(60_000 + i),
        Int = int.MinValue + i,
        UInt = uint.MaxValue - (uint)i,
        Long = long.MinValue + i,
        ULong = ulong.MaxValue - (ulong)i,
        Float = i + 0.5f,
        Double = Math.PI * i,
        Decimal = 123456789.123456789m + i,
        Money = 1234567.891m - i,
        Text = $"text {i} ✓",
        Char = (char)('a' + (i % 26)),
        Bytes = [(byte)i, 0, 255],
        At = new DateTime(2025, 3, 4, 5, 6, 7, DateTimeKind.Utc).AddTicks(i * 10),
        Offset = new DateTimeOffset(2025, 3, 4, 5, 6, 7, TimeSpan.Zero).AddMinutes(i),
        Day = new DateOnly(2025, 3, 4).AddDays(i),
        Time = new TimeOnly(13, 14, 15).Add(TimeSpan.FromTicks(i * 10)),
        Span = TimeSpan.FromMilliseconds(i * 1.5),
        Key = new Guid(i, 2, 3, [4, 5, 6, 7, 8, 9, 10, 11]),
        Tier = (Tier)(i % 3),
        MaybeInt = i % 2 == 0 ? null : i,
        MaybeDouble = i % 2 == 0 ? null : i / 4.0,
        MaybeDecimal = i % 2 == 0 ? null : i / 8m,
        MaybeText = i % 2 == 0 ? null : $"maybe {i}",
        MaybeBytes = i % 2 == 0 ? null : [(byte)i],
        MaybeAt = i % 2 == 0 ? null : new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(i),
        MaybeOffset = i % 2 == 0 ? null : new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero).AddDays(i),
        MaybeDay = i % 2 == 0 ? null : new DateOnly(2024, 1, 1).AddDays(i),
        MaybeTime = i % 2 == 0 ? null : new TimeOnly(1, 2, 3),
        MaybeSpan = i % 2 == 0 ? null : TimeSpan.FromHours(i),
        MaybeKey = i % 2 == 0 ? null : Guid.Empty,
        MaybeTier = i % 2 == 0 ? null : Tier.Gold,
    };
}
#pragma warning restore CA1720

public sealed record ListRow
{
    public int Id { get; init; }
    public int[] Scores { get; init; } = [];
    public List<string?>? Tags { get; init; }
    public IReadOnlyList<DateTime?>? Times { get; init; }
    public List<decimal>? Amounts { get; init; }
    public Guid[]? Keys { get; init; }
    public List<Tier>? Tiers { get; init; }
    public TimeOnly[]? Clock { get; init; }
}

public sealed class AttributedOrder
{
    [ParquetColumn("order_id")]
    public int Id { get; set; }

    [Column("customer_name")]
    public string Customer { get; set; } = string.Empty;

    [Column("shared_wins")]
    [ParquetColumn("parquet_loses")]
    public string Both { get; set; } = string.Empty;

    [ParquetColumn(Ignore = true)]
    public string Secret { get; set; } = "unset";

    [IgnoreColumn]
    public string Internal { get; set; } = "unset";
}

public sealed class RequiredOrder
{
    public required int Id { get; init; }

    public string? Customer { get; init; }
}
