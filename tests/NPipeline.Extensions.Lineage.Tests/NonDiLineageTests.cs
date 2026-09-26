using System.Collections.Concurrent;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Lineage.Tests;

/// <summary>
///     Lineage without a DI container. A bare runner used <see cref="NullLineage" /> and core's default lineage factory can't
///     build reports, so pipeline lineage sinks never received anything outside DI, whatever was configured.
/// </summary>
public sealed class NonDiLineageTests
{
    private const string PipelineSinkKey = "testing.pipeline.lineage.sink";
    private const string ItemSinkKey = "testing.item.lineage.sink";
    private const string LoggerFactoryKey = "testing.logger.factory";
    private const string SinkCategory = "NPipeline.Lineage.LoggingPipelineLineageSink";

    [Fact]
    public async Task UseLineage_WithPipelineLineageSink_ReceivesReport()
    {
        // Arrange
        var sink = new RecordingPipelineLineageSink();
        var runner = new PipelineRunnerBuilder().UseLineage().Build();
        await using var context = new PipelineContext();
        context.Items[PipelineSinkKey] = sink;

        // Act
        await runner.RunAsync<ReportPipeline>(context);

        // Assert
        var report = sink.Reports.Should().ContainSingle().Subject;
        report.Pipeline.Should().Be(nameof(ReportPipeline));
        report.Nodes.Select(n => n.Id).Should().BeEquivalentTo("source", "sink");
        report.Edges.Should().ContainSingle(e => e.From == "source" && e.To == "sink");
    }

    [Fact]
    public async Task UseLineage_WithLoggingSinkAndExplicitLoggerFactory_LogsReport()
    {
        // Arrange
        var provider = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(provider));
        var runner = new PipelineRunnerBuilder().UseLineage().Build();
        await using var context = new PipelineContext();
        context.Items[LoggerFactoryKey] = loggerFactory;

        // Act
        await runner.RunAsync<LoggingSinkPipeline>(context);

        // Assert
        provider.Entries.Should().ContainSingle(e => e.Category == SinkCategory && e.Level == LogLevel.Information)
            .Which.Message.Should().Contain(nameof(LoggingSinkPipeline));
    }

    [Fact]
    public async Task UseLineage_WithLoggingSinkByType_LogsReportThroughContextLogging()
    {
        // Arrange - the Lineage README's example: UseLoggingPipelineLineageSink() with the context's logging.
        var provider = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(provider));
        var runner = new PipelineRunnerBuilder().UseLineage().Build();
        await using var context = new PipelineContext(PipelineContextConfiguration.WithLogging(loggerFactory));

        // Act
        await runner.RunAsync<LoggingSinkPipeline>(context);

        // Assert
        provider.Entries.Should().ContainSingle(e => e.Category == SinkCategory && e.Level == LogLevel.Information)
            .Which.Message.Should().Contain(nameof(LoggingSinkPipeline));
    }

    [Fact]
    public async Task UseLineage_WithItemLevelLineage_FeedsItemSink()
    {
        // Arrange
        var itemSink = new RecordingLineageSink();
        var runner = new PipelineRunnerBuilder().UseLineage().Build();
        await using var context = new PipelineContext();
        context.Items[ItemSinkKey] = itemSink;

        // Act
        await runner.RunAsync<ItemLevelLineagePipeline>(context);

        // Assert
        itemSink.Records.Select(r => r.CorrelationId).Distinct().Should().HaveCount(3);
    }

    [Fact]
    public async Task UseLineage_WithCustomFactoryReturningNoReport_DoesNotGenerateOne()
    {
        // Arrange - only core's default factory is filled in; any other factory's null is a deliberate "no report".
        var sink = new RecordingPipelineLineageSink();
        var runner = new PipelineRunnerBuilder().UseLineage().Build();
        await using var context = new PipelineContext(PipelineContextConfiguration.Default with { LineageFactory = new NoReportLineageFactory() });
        context.Items[PipelineSinkKey] = sink;

        // Act
        await runner.RunAsync<ReportPipeline>(context);

        // Assert
        sink.Reports.Should().BeEmpty();
    }

    [Fact]
    public async Task WithoutUseLineage_WithPipelineLineageSink_WarnsAndDoesNotReport()
    {
        // Arrange
        var provider = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(provider));
        var sink = new RecordingPipelineLineageSink();
        var runner = PipelineRunner.Create();
        await using var context = new PipelineContext(PipelineContextConfiguration.WithLogging(loggerFactory));
        context.Items[PipelineSinkKey] = sink;

        // Act
        await runner.RunAsync<ReportPipeline>(context);

        // Assert
        sink.Reports.Should().BeEmpty();

        provider.Entries.Should().ContainSingle(e => e.Level == LogLevel.Warning)
            .Which.Message.Should().Contain(nameof(RecordingPipelineLineageSink)).And.Contain("UseLineage()");
    }

    [Fact]
    public async Task WithoutUseLineage_WithoutPipelineLineageSink_DoesNotWarn()
    {
        // Arrange
        var provider = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(provider));
        var runner = PipelineRunner.Create();
        await using var context = new PipelineContext(PipelineContextConfiguration.WithLogging(loggerFactory));

        // Act
        await runner.RunAsync<ReportPipeline>(context);

        // Assert
        provider.Entries.Should().NotContain(e => e.Level >= LogLevel.Warning);
    }

    [Fact]
    public void UseLineage_WithNullBuilder_Throws()
    {
        // Act
        var act = () => PipelineRunnerBuilderLineageExtensions.UseLineage(null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    /// <summary>Two-node pipeline that attaches a pipeline-level sink when the test seeded one.</summary>
    public sealed class ReportPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            if (context.Items.TryGetValue(PipelineSinkKey, out var seeded) && seeded is IPipelineLineageSink pipelineSink)
                _ = builder.AddPipelineLineageSink(pipelineSink);

            var source = builder.AddSource<NumbersSourceNode, int>("source");
            var sink = builder.AddSink<CollectingSinkNode, int>("sink");
            _ = builder.Connect(source, sink);
        }
    }

    /// <summary>
    ///     Uses the logging sink: with the seeded logger factory when there is one, otherwise registered by type.
    /// </summary>
    public sealed class LoggingSinkPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            _ = context.Items.TryGetValue(LoggerFactoryKey, out var seeded) && seeded is ILoggerFactory loggerFactory
                ? builder.UseLoggingPipelineLineageSink(loggerFactory)
                : builder.UseLoggingPipelineLineageSink();

            var source = builder.AddSource<NumbersSourceNode, int>("source");
            var sink = builder.AddSink<CollectingSinkNode, int>("sink");
            _ = builder.Connect(source, sink);
        }
    }

    /// <summary>Item-level lineage on, with an explicitly configured item-level sink.</summary>
    public sealed class ItemLevelLineagePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            _ = builder.EnableItemLevelLineage();
            _ = builder.AddLineageSink((ILineageSink)context.Items[ItemSinkKey]!);
            var source = builder.AddSource<NumbersSourceNode, int>("source");
            var sink = builder.AddSink<CollectingSinkNode, int>("sink");
            _ = builder.Connect(source, sink);
        }
    }

    public sealed class NumbersSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
            => new InMemoryDataStream<int>([1, 2, 3], "numbers");
    }

    public sealed class CollectingSinkNode : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }

    private sealed class NoReportLineageFactory : ILineageFactory
    {
        public ILineageSink? CreateLineageSink(Type sinkType) => null;

        public IPipelineLineageSink? CreatePipelineLineageSink(Type sinkType) => null;

        public IPipelineLineageSinkProvider? ResolvePipelineLineageSinkProvider() => null;

        public ILineageCollector? ResolveLineageCollector() => null;

        public PipelineLineageReport? CreateLineageReport(string pipelineName, Guid pipelineId, PipelineGraph graph, Guid runId) => null;
    }

    private sealed class RecordingLineageSink : ILineageSink
    {
        private readonly ConcurrentQueue<LineageRecord> _records = new();

        public IReadOnlyCollection<LineageRecord> Records => _records;

        public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken)
        {
            _records.Enqueue(record);
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingPipelineLineageSink : IPipelineLineageSink
    {
        private readonly ConcurrentQueue<PipelineLineageReport> _reports = new();

        public IReadOnlyCollection<PipelineLineageReport> Reports => _reports;

        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
        {
            _reports.Enqueue(report);
            return Task.CompletedTask;
        }
    }

    private sealed record CapturedEntry(string Category, LogLevel Level, string Message);

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<CapturedEntry> _entries = new();

        public IReadOnlyCollection<CapturedEntry> Entries => _entries;

        public ILogger CreateLogger(string categoryName) => new CategoryLogger(categoryName, _entries);

        public void Dispose()
        {
        }

        private sealed class CategoryLogger(string category, ConcurrentQueue<CapturedEntry> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter) =>
                entries.Enqueue(new CapturedEntry(category, logLevel, formatter(state, exception)));
        }
    }
}
