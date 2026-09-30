using NPipeline.Connectors.DataLake.FormatAdapters;
using NPipeline.Connectors.DataLake.Partitioning;
using NPipeline.Connectors.Parquet;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using InMemoryStorageProvider = NPipeline.Tests.Common.InMemoryStorageProvider;
using Parquet;

namespace NPipeline.Connectors.DataLake.Tests;

/// <summary>The Data Lake connector on the rewritten Parquet connector: compaction keeps values, and file URIs keep the table's parameters.</summary>
public sealed class DataLakeParquetMigrationTests
{
    [Fact]
    public async Task Compaction_rewrites_every_value_of_every_type()
    {
        var provider = new InMemoryStorageProvider();
        var table = InMemoryStorageProvider.Uri("table");
        var spec = PartitionSpec<WideRecord>.By(x => x.Region);
        var records = Enumerable.Range(0, 60).Select(WideRecord.Create).ToList();
        var options = new DataLakeParquetOptions { RowGroupSize = 7, Codec = CompressionMethod.Zstd };

        // Six appends of ten rows: small files in two partitions.
        foreach (var chunk in records.Chunk(10))
        {
            await using var writer = new DataLakeTableWriter<WideRecord>(provider, table, spec, options);
            await writer.AppendAsync(new InMemoryDataStream<WideRecord>([.. chunk]), CancellationToken.None);
        }

        var result = await new DataLakeCompactor(provider, table, options).CompactAsync(new TableCompactRequest
        {
            TableBasePath = table,
            Provider = provider,
            MinFilesToCompact = 2,
            DeleteOriginalFiles = false,
        });

        result.FilesCompacted.Should().BeGreaterThan(0);
        result.RowsProcessed.Should().Be(60);

        // The compacted files hold the same rows as the files they replace.
        var compacted = new List<WideRecord>();

        foreach (var file in result.NewFiles!)
        {
            var source = ParquetConnector.Source<WideRecord>(table.Combine(file), o => o with { Provider = provider });
            compacted.AddRange(await source.OpenStream(PipelineContext.CreateDefault(), CancellationToken.None).ToListAsync());
        }

        compacted.OrderBy(r => r.Id).Should().BeEquivalentTo(records, o => o.WithStrictOrdering());
    }

    [Fact]
    public async Task Data_files_keep_the_table_uri_parameters()
    {
        var provider = new InMemoryStorageProvider();
        var table = InMemoryStorageProvider.Uri("table").WithParameter("token", "abc");
        var spec = PartitionSpec<WideRecord>.By(x => x.Region);

        for (var append = 0; append < 3; append++)
        {
            await using var writer = new DataLakeTableWriter<WideRecord>(provider, table, spec);
            await writer.AppendAsync(new InMemoryDataStream<WideRecord>([.. Enumerable.Range(append * 10, 10).Select(WideRecord.Create)]), CancellationToken.None);
        }

        _ = await new DataLakeCompactor(provider, table).CompactAsync(new TableCompactRequest
        {
            TableBasePath = table,
            Provider = provider,
            MinFilesToCompact = 2,
            DeleteOriginalFiles = false,
        });

        var parquetWrites = provider.WriteRequests.Where(u => u.Path.EndsWith(".parquet", StringComparison.Ordinal)).ToList();
        parquetWrites.Should().NotBeEmpty();
        parquetWrites.Should().OnlyContain(u => u.Parameters.ContainsKey("token") && u.Parameters["token"] == "abc");

        var rows = await new DataLakeTableSourceNode<WideRecord>(provider, table)
            .OpenStream(PipelineContext.CreateDefault(), CancellationToken.None)
            .ToListAsync();

        rows.Select(r => r.Id).Distinct().Should().HaveCount(30);
    }

    [Fact]
    public void Options_are_validated()
    {
        var provider = new InMemoryStorageProvider();
        var table = InMemoryStorageProvider.Uri("table");

        var act = () => new DataLakeTableWriter<WideRecord>(provider, table, null, new DataLakeParquetOptions { RowGroupSize = 0 });

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("RowGroupSize");
    }

    public enum Tier
    {
        Bronze,
        Silver,
        Gold,
    }

    public sealed record WideRecord
    {
        public int Id { get; init; }
        public string Region { get; init; } = string.Empty;
        public long Big { get; init; }
        public double Ratio { get; init; }
        public bool Active { get; init; }

        [ParquetDecimal(18, 4)]
        public decimal Amount { get; init; }

        public decimal? Discount { get; init; }
        public DateTime At { get; init; }
        public DateTimeOffset Offset { get; init; }
        public DateOnly Day { get; init; }
        public TimeOnly Time { get; init; }
        public TimeSpan Elapsed { get; init; }
        public Guid Key { get; init; }
        public Tier Level { get; init; }
        public Tier? MaybeLevel { get; init; }
        public int? MaybeInt { get; init; }
        public string? Note { get; init; }
        public byte[]? Blob { get; init; }
        public uint Counter { get; init; }
        public List<string?>? Tags { get; init; }
        public int[] Scores { get; init; } = [];

        public static WideRecord Create(int i) => new()
        {
            Id = i,
            Region = i % 2 == 0 ? "EU" : "US",
            Big = long.MaxValue - i,
            Ratio = i / 3.0,
            Active = i % 3 == 0,
            Amount = 1234.5678m + i,
            Discount = i % 4 == 0 ? null : i / 100m,
            At = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc).AddMinutes(i),
            Offset = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero).AddHours(i),
            Day = new DateOnly(2025, 1, 1).AddDays(i),
            Time = new TimeOnly(1, 2, 3).Add(TimeSpan.FromSeconds(i)),
            Elapsed = TimeSpan.FromMilliseconds(i * 1.5),
            Key = new Guid(i, 1, 2, [3, 4, 5, 6, 7, 8, 9, 10]),
            Level = (Tier)(i % 3),
            MaybeLevel = i % 5 == 0 ? null : (Tier)(i % 3),
            MaybeInt = i % 2 == 0 ? i : null,
            Note = i % 3 == 0 ? null : $"note {i}",
            Blob = i % 4 == 0 ? null : [(byte)i, 1, 2],
            Counter = uint.MaxValue - (uint)i,
            Tags = (i % 4) switch
            {
                0 => null,
                1 => [],
                2 => ["a", null, "c"],
                _ => [$"t{i}"],
            },
            Scores = [.. Enumerable.Range(0, i % 4)],
        };
    }
}
