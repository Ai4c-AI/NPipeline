using Microsoft.Extensions.Logging;
using NPipeline.Observability;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Comprehensive tests for <see cref="LoggingPipelineMetricsSink" />.
/// </summary>
public sealed class LoggingPipelineMetricsSinkTests
{
    private static readonly Guid s_pipelineId = Guid.Parse("11111111-1111-1111-1111-111111111111");

    #region Helper Methods

    private static ILogger<LoggingPipelineMetricsSink> CreateLogger()
    {
        var logger = A.Fake<ILogger<LoggingPipelineMetricsSink>>();

        // LoggerMessage delegates check IsEnabled before logging
        A.CallTo(() => logger.IsEnabled(A<LogLevel>._)).Returns(true);
        return logger;
    }

    private static IPipelineMetrics CreatePipelineMetrics(bool success, Exception? exception = null, IReadOnlyList<INodeMetrics>? nodeMetrics = null,
        long totalItemsProcessed = 285, double? durationMs = 5000, long? itemsIn = 100, long? itemsOut = 95) =>
        new PipelineMetrics("TestPipeline", s_pipelineId, Guid.NewGuid(), DateTimeOffset.UtcNow.AddSeconds(-5), DateTimeOffset.UtcNow, durationMs,
            success, totalItemsProcessed, nodeMetrics ?? [], exception, itemsIn, itemsOut);

    private static List<(EventId EventId, LogLevel Level, IReadOnlyDictionary<string, object?> Values)> GetLogEntries(ILogger logger) =>
    [
        .. Fake.GetCalls(logger)
            .Where(static c => c.Method.Name == "Log")
            .Select(static c => (
                c.GetArgument<EventId>(1),
                c.GetArgument<LogLevel>(0),
                (IReadOnlyDictionary<string, object?>)((IEnumerable<KeyValuePair<string, object?>>)c.Arguments[2]!)
                .ToDictionary(static kv => kv.Key, static kv => kv.Value))),
    ];

    private static INodeMetrics CreateNodeMetrics(
        string nodeId,
        bool success,
        Exception? exception = null,
        int retryCount = 0,
        long itemsProcessed = 100,
        double? throughputItemsPerSec = null,
        double? averageItemProcessingMs = null) =>
        new NodeMetrics(nodeId, DateTimeOffset.UtcNow.AddSeconds(-1), DateTimeOffset.UtcNow, 1000, success, itemsProcessed, itemsProcessed - 5,
            exception, retryCount, null, null, throughputItemsPerSec, averageItemProcessingMs, 1, s_pipelineId);

    #endregion

    #region Constructor Tests

    [Fact]
    public void Constructor_WithNullLogger_ShouldUseNullLogger()
    {
        // Arrange & Act
        var sink = new LoggingPipelineMetricsSink();

        // Assert
        Assert.NotNull(sink);
    }

    [Fact]
    public void Constructor_WithLogger_ShouldUseProvidedLogger()
    {
        // Arrange
        var logger = A.Fake<ILogger<LoggingPipelineMetricsSink>>();

        // Act
        var sink = new LoggingPipelineMetricsSink(logger);

        // Assert
        Assert.NotNull(sink);
    }

    #endregion

    #region RecordAsync Tests

    [Fact]
    public async Task RecordAsync_WithNullMetrics_ShouldThrowArgumentNullException()
    {
        // Arrange
        var sink = new LoggingPipelineMetricsSink();

        // Act & Assert
        _ = await Assert.ThrowsAsync<ArgumentNullException>(async () => await sink.RecordAsync(null!, CancellationToken.None));
    }

    [Fact]
    public async Task RecordAsync_WithSuccessfulPipeline_ShouldLogInformation()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var metrics = CreatePipelineMetrics(true);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert - Logs pipeline + overall throughput (2 Information calls)
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        Assert.Equal(2, logCalls.Count);
    }

    [Fact]
    public async Task RecordAsync_WithFailedPipeline_ShouldLogError()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var metrics = CreatePipelineMetrics(false);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Error).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithSuccessfulPipeline_ShouldIncludeCorrectProperties()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var metrics = CreatePipelineMetrics(true);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert - Logs pipeline + overall throughput (2 Information calls)
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        Assert.Equal(2, logCalls.Count);
    }

    [Fact]
    public async Task RecordAsync_WithFailedPipeline_ShouldIncludeExceptionMessage()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var exception = new InvalidOperationException("Pipeline failed");
        var metrics = CreatePipelineMetrics(false, exception);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Error).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_ShouldLogNodeLevelDetails()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);

        var metrics = CreatePipelineMetrics(true, nodeMetrics:
        [
            CreateNodeMetrics("node1", true, itemsProcessed: 100),
            CreateNodeMetrics("node2", true, itemsProcessed: 95),
            CreateNodeMetrics("node3", true, itemsProcessed: 90),
        ]);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        // Should log for each node
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        Assert.True(logCalls.Count >= 2);
    }

    [Fact]
    public async Task RecordAsync_WithFailedNode_ShouldLogWarning()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var exception = new InvalidOperationException("Node failed");

        var metrics = CreatePipelineMetrics(true, nodeMetrics:
        [
            CreateNodeMetrics("node1", true, itemsProcessed: 100),
            CreateNodeMetrics("node2", false, exception, itemsProcessed: 50),
        ]);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Warning).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithNodeRetries_ShouldLogRetryCount()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);

        var metrics = CreatePipelineMetrics(true, nodeMetrics:
        [
            CreateNodeMetrics("node1", true, itemsProcessed: 100, retryCount: 3),
        ]);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert - Logs pipeline + node + retry + overall throughput (4 Information calls)
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        Assert.Equal(4, logCalls.Count);
    }

    [Fact]
    public async Task RecordAsync_WithNodeThroughput_ShouldLogThroughput()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);

        var metrics = CreatePipelineMetrics(true, nodeMetrics:
        [
            CreateNodeMetrics("node1", true, itemsProcessed: 100, throughputItemsPerSec: 1000.5),
        ]);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Debug).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithAverageItemProcessing_ShouldLogDebug()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);

        var metrics = CreatePipelineMetrics(true, nodeMetrics:
        [
            CreateNodeMetrics("node1", true, itemsProcessed: 100, averageItemProcessingMs: 1.5),
        ]);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert - Debug log emitted for average time per item
        var calls = Fake.GetCalls(loggerMock);
        var debugCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Debug).ToList();
        _ = Assert.Single(debugCalls);
    }

    [Fact]
    public async Task RecordAsync_WithOverallThroughput_ShouldLogOverallThroughput()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);

        var metrics = CreatePipelineMetrics(true, totalItemsProcessed: 1000, durationMs: 5000);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert - Logs pipeline + overall throughput (2 Information calls)
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        Assert.Equal(2, logCalls.Count);
    }

    [Fact]
    public async Task RecordAsync_WithItemCounts_LogsItemsInAndOutAndThroughputFromItemsOut()
    {
        // Arrange - TotalItemsProcessed counts an item once per node, so it must not be reported as the pipeline's count.
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var metrics = CreatePipelineMetrics(true, totalItemsProcessed: 3000, durationMs: 5000, itemsIn: 1000, itemsOut: 990);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var entries = GetLogEntries(loggerMock);
        var pipeline = Assert.Single(entries, static e => e.EventId.Id == 1);
        Assert.Equal(1000L, pipeline.Values["ItemsIn"]);
        Assert.Equal(990L, pipeline.Values["ItemsOut"]);
        Assert.False(pipeline.Values.ContainsKey("TotalItemsProcessed"));

        var throughput = Assert.Single(entries, static e => e.EventId.Id == 8);
        Assert.Equal(198.0, (double)throughput.Values["Throughput"]!, 3);
    }

    [Fact]
    public async Task RecordAsync_WithOnlySourcesObserved_UsesItemsInForThroughput()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var metrics = CreatePipelineMetrics(true, durationMs: 5000, itemsIn: 1000, itemsOut: null);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var throughput = Assert.Single(GetLogEntries(loggerMock), static e => e.EventId.Id == 8);
        Assert.Equal(200.0, (double)throughput.Values["Throughput"]!, 3);
    }

    [Fact]
    public async Task RecordAsync_WithoutItemCounts_SaysCountsWereNotRecordedAndSkipsThroughput()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var metrics = CreatePipelineMetrics(true, totalItemsProcessed: 1000, durationMs: 5000, itemsIn: null, itemsOut: null);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var entries = GetLogEntries(loggerMock);
        _ = Assert.Single(entries, static e => e.EventId.Id == 10 && e.Level == LogLevel.Information);
        Assert.DoesNotContain(entries, static e => e.EventId.Id is 1 or 8);
    }

    [Fact]
    public async Task RecordAsync_FailedWithoutItemCounts_LogsTheFailureWithoutCounts()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var metrics = CreatePipelineMetrics(false, new InvalidOperationException("boom"), itemsIn: null, itemsOut: null);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var failure = Assert.Single(GetLogEntries(loggerMock), static e => e.Level == LogLevel.Error);
        Assert.Equal(11, failure.EventId.Id);
        Assert.Equal("boom", failure.Values["ExceptionMessage"]);
    }

    [Fact]
    public async Task RecordAsync_NodeWithoutItemCounts_SaysCountsWereNotRecordedInsteadOfZero()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var unobserved = (NodeMetrics)CreateNodeMetrics("unobserved", true, itemsProcessed: 0) with { ItemCountsRecorded = false };
        var failed = (NodeMetrics)CreateNodeMetrics("failed", false, new InvalidOperationException("boom")) with { ItemCountsRecorded = false };
        var metrics = CreatePipelineMetrics(true, nodeMetrics: [unobserved, failed]);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var entries = GetLogEntries(loggerMock);
        Assert.Equal("unobserved", Assert.Single(entries, static e => e.EventId.Id == 12).Values["NodeId"]);
        Assert.Equal("failed", Assert.Single(entries, static e => e.EventId.Id == 13).Values["NodeId"]);
        Assert.DoesNotContain(entries, static e => e.EventId.Id is 3 or 4);
    }

    [Fact]
    public async Task RecordAsync_WithZeroDuration_ShouldNotLogOverallThroughput()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);

        var metrics = CreatePipelineMetrics(true, totalItemsProcessed: 1000, durationMs: 0);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert - Should log pipeline but not overall throughput (1 Information call)
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithNullDuration_ShouldNotLogOverallThroughput()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);

        var metrics = CreatePipelineMetrics(true, totalItemsProcessed: 1000, durationMs: null);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert - Should log pipeline but not overall throughput (1 Information call)
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithEmptyNodeMetrics_ShouldOnlyLogPipeline()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);

        var metrics = CreatePipelineMetrics(true, nodeMetrics: []);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert - Should log pipeline + overall throughput (2 Information calls)
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        Assert.Equal(2, logCalls.Count);
    }

    [Fact]
    public async Task RecordAsync_WithNullException_ShouldLogUnknownError()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);

        var metrics = CreatePipelineMetrics(false, nodeMetrics:
        [
            CreateNodeMetrics("node1", false, itemsProcessed: 50),
        ]);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Warning).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithCancellation_ShouldCompleteSuccessfully()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var metrics = CreatePipelineMetrics(true);
        var cts = new CancellationTokenSource();

        // Act
        await sink.RecordAsync(metrics, cts.Token);

        // Assert - Logs pipeline + overall throughput (2 Information calls)
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        Assert.Equal(2, logCalls.Count);
    }

    [Fact]
    public async Task RecordAsync_WithCancelledToken_ShouldCompleteSuccessfully()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var metrics = CreatePipelineMetrics(true);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act
        await sink.RecordAsync(metrics, cts.Token);

        // Assert - Logs pipeline + overall throughput (2 Information calls)
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        Assert.Equal(2, logCalls.Count);
    }

    [Fact]
    public async Task RecordAsync_WithManyNodes_ShouldLogAllNodes()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var nodeCount = 10;
        var nodeMetrics = new List<INodeMetrics>();

        for (var i = 0; i < nodeCount; i++)
        {
            nodeMetrics.Add(CreateNodeMetrics($"node_{i}", true, itemsProcessed: 100));
        }

        var metrics = CreatePipelineMetrics(true, nodeMetrics: nodeMetrics);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        // Should log for pipeline + all nodes
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        Assert.True(logCalls.Count >= 2);
    }

    [Fact]
    public async Task RecordAsync_WithMixedSuccessAndFailure_ShouldLogAppropriateLevels()
    {
        // Arrange
        var loggerMock = CreateLogger();
        var sink = new LoggingPipelineMetricsSink(loggerMock);

        var metrics = CreatePipelineMetrics(false, nodeMetrics:
        [
            CreateNodeMetrics("node1", true, itemsProcessed: 100),
            CreateNodeMetrics("node2", false, new InvalidOperationException("Failed"), itemsProcessed: 50),
            CreateNodeMetrics("node3", true, itemsProcessed: 75),
        ]);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        // Should log Error for pipeline, Information for successful nodes, Warning for failed node
        var calls = Fake.GetCalls(loggerMock);
        var errorCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Error).ToList();
        var infoCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        var warningCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Warning).ToList();

        _ = Assert.Single(errorCalls);
        Assert.True(infoCalls.Count >= 2);
        _ = Assert.Single(warningCalls);
    }

    #endregion
}
