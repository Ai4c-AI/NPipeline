using System.Collections.Concurrent;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Lineage;
using NPipeline.Lineage.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Lineage.Tests;

/// <summary>
///     <see cref="PipelineBuilderLineageExtensions.UseLoggingPipelineLineageSink(PipelineBuilder)" /> must log through the run's
///     logging. It used to construct the sink with no logger, so every report went to a null logger.
/// </summary>
public sealed class PipelineBuilderLineageExtensionsTests
{
    private const string SinkCategory = "NPipeline.Lineage.LoggingPipelineLineageSink";

    [Fact]
    public async Task UseLoggingPipelineLineageSink_UnderDependencyInjection_LogsThroughTheContainersLogging()
    {
        // Arrange
        var provider = new CapturingLoggerProvider();
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddProvider(provider));
        services.AddNPipeline(typeof(PipelineBuilderLineageExtensionsTests).Assembly);
        services.AddNPipelineLineage();
        await using var serviceProvider = services.BuildServiceProvider();

        // Act
        await serviceProvider.RunPipelineAsync<DefaultLoggingSinkPipeline>();

        // Assert
        provider.Entries.Should().ContainSingle(e => e.Category == SinkCategory && e.Level == LogLevel.Information)
            .Which.Message.Should().Contain(nameof(DefaultLoggingSinkPipeline));
    }

    [Fact]
    public async Task UseLoggingPipelineLineageSink_WithLoggerFactory_LogsThroughThatLoggerFactory()
    {
        // Arrange - the container has no logging, so the report can only arrive through the factory the pipeline passes.
        ExplicitLoggingSinkPipeline.Provider = new CapturingLoggerProvider();
        var services = new ServiceCollection();
        services.AddNPipeline(typeof(PipelineBuilderLineageExtensionsTests).Assembly);
        services.AddNPipelineLineage();
        await using var serviceProvider = services.BuildServiceProvider();

        // Act
        await serviceProvider.RunPipelineAsync<ExplicitLoggingSinkPipeline>();

        // Assert
        ExplicitLoggingSinkPipeline.Provider.Entries.Should()
            .ContainSingle(e => e.Category == SinkCategory && e.Level == LogLevel.Information)
            .Which.Message.Should().Contain(nameof(ExplicitLoggingSinkPipeline));
    }

    [Fact]
    public void UseLoggingPipelineLineageSink_WithNullLoggerFactory_Throws()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var act = () => builder.UseLoggingPipelineLineageSink(null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    private sealed class DefaultLoggingSinkPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            builder.UseLoggingPipelineLineageSink();
            var source = builder.AddSource<InMemorySourceNode<string>, string>("source");
            var sink = builder.AddSink<DiscardingSink, string>("sink");
            builder.Connect(source, sink);
        }
    }

    private sealed class ExplicitLoggingSinkPipeline : IPipelineDefinition
    {
        public static CapturingLoggerProvider Provider { get; set; } = new();

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var loggerFactory = LoggerFactory.Create(b => b.AddProvider(Provider));
            builder.UseLoggingPipelineLineageSink(loggerFactory);
            var source = builder.AddSource<InMemorySourceNode<string>, string>("source");
            var sink = builder.AddSink<DiscardingSink, string>("sink");
            builder.Connect(source, sink);
        }
    }

    public sealed class DiscardingSink : SinkNode<string>
    {
        public override async Task ConsumeAsync(IDataStream<string> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
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
