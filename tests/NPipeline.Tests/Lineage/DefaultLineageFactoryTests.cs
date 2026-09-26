using AwesomeAssertions;
using FakeItEasy;
using Microsoft.Extensions.Logging;
using NPipeline.Extensions.Testing;
using NPipeline.Lineage;

namespace NPipeline.Tests.Lineage;

/// <summary>
///     Tests for <see cref="DefaultLineageFactory" /> covering lineage sink creation and resolution.
/// </summary>
public sealed class DefaultLineageFactoryTests
{
    private readonly DefaultLineageFactory _factory = new();

    #region CreateLineageSink Tests

    [Fact]
    public void CreateLineageSink_WithValidLineageSinkType_ReturnsInstance()
    {
        // Arrange
        var sinkType = typeof(TestLineageSink);

        // Act
        var result = _factory.CreateLineageSink(sinkType);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<TestLineageSink>();
    }

    [Fact]
    public void CreateLineageSink_WithNonImplementingType_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(double);

        // Act
        var result = _factory.CreateLineageSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreateLineageSink_WithTypeWithoutParameterlessConstructor_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(TypeWithoutParameterlessConstructor);

        // Act
        var result = _factory.CreateLineageSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreateLineageSink_WithThrowingConstructor_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(ThrowingLineageSinkConstructor);

        // Act
        var result = _factory.CreateLineageSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    #endregion

    #region CreatePipelineLineageSink Tests

    [Fact]
    public void CreatePipelineLineageSink_WithValidPipelineLineageSinkType_ReturnsInstance()
    {
        // Arrange
        var sinkType = typeof(TestPipelineLineageSink);

        // Act
        var result = _factory.CreatePipelineLineageSink(sinkType);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<TestPipelineLineageSink>();
    }

    [Fact]
    public void CreatePipelineLineageSink_WithNonImplementingType_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(DateTime);

        // Act
        var result = _factory.CreatePipelineLineageSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreatePipelineLineageSink_WithTypeWithoutParameterlessConstructor_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(TypeWithoutParameterlessConstructor);

        // Act
        var result = _factory.CreatePipelineLineageSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreatePipelineLineageSink_MultipleInvocations_CreatesNewInstanceEachTime()
    {
        // Arrange
        var sinkType = typeof(TestPipelineLineageSink);

        // Act
        var result1 = _factory.CreatePipelineLineageSink(sinkType);
        var result2 = _factory.CreatePipelineLineageSink(sinkType);

        // Assert
        result1.Should().NotBeNull();
        result2.Should().NotBeNull();
        result1.Should().NotBeSameAs(result2);
    }

    #endregion

    #region Constructor Argument Tests

    [Fact]
    public void CreatePipelineLineageSink_WithOptionalGenericLogger_SuppliesLoggerFromLoggerFactory()
    {
        // Arrange - the sink's only constructor takes optional parameters, so it has no parameterless constructor.
        var logger = new CapturingLogger();
        var loggerFactory = A.Fake<ILoggerFactory>();
        A.CallTo(() => loggerFactory.CreateLogger(A<string>._)).Returns(logger);
        var factory = new DefaultLineageFactory(loggerFactory);

        // Act
        var result = factory.CreatePipelineLineageSink(typeof(OptionalLoggerPipelineLineageSink));

        // Assert
        var sink = result.Should().BeOfType<OptionalLoggerPipelineLineageSink>().Subject;
        sink.Logger.Should().NotBeNull();
        sink.Name.Should().Be("default", "optional parameters that are not loggers take their default values");
        sink.Logger!.LogInformation("hello");
        logger.LogEntries.Should().ContainSingle(e => e.Message == "hello");
        A.CallTo(() => loggerFactory.CreateLogger(A<string>.That.EndsWith(nameof(OptionalLoggerPipelineLineageSink)))).MustHaveHappened();
    }

    [Fact]
    public void CreateLineageSink_WithLoggerFactoryAndLoggerParameters_SuppliesBoth()
    {
        // Arrange
        var loggerFactory = A.Fake<ILoggerFactory>();
        var factory = new DefaultLineageFactory(loggerFactory);

        // Act
        var result = factory.CreateLineageSink(typeof(LoggerFactoryLineageSink));

        // Assert
        var sink = result.Should().BeOfType<LoggerFactoryLineageSink>().Subject;
        sink.LoggerFactory.Should().BeSameAs(loggerFactory);
        sink.Logger.Should().NotBeNull();
    }

    [Fact]
    public void CreatePipelineLineageSink_WithRequiredNonLoggerParameter_ReturnsNull()
    {
        // Act
        var result = _factory.CreatePipelineLineageSink(typeof(RequiredArgumentPipelineLineageSink));

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreatePipelineLineageSink_WithSeveralConstructors_UsesLongestSatisfiableConstructor()
    {
        // Act
        var result = _factory.CreatePipelineLineageSink(typeof(MultiConstructorPipelineLineageSink));

        // Assert
        result.Should().BeOfType<MultiConstructorPipelineLineageSink>().Which.UsedLoggerConstructor.Should().BeTrue();
    }

    #endregion

    #region Resolution Tests

    [Fact]
    public void ResolvePipelineLineageSinkProvider_WithoutDIContainer_ReturnsNull()
    {
        // Act
        var result = _factory.ResolvePipelineLineageSinkProvider();

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void ResolveLineageCollector_WithoutDIContainer_ReturnsNull()
    {
        // Act
        var result = _factory.ResolveLineageCollector();

        // Assert
        result.Should().BeNull();
    }

    #endregion

    #region Test Fixtures

    private sealed class TestLineageSink : ILineageSink
    {
        public Task RecordAsync(
            LineageRecord record,
            CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class TestPipelineLineageSink : IPipelineLineageSink
    {
        public Task RecordAsync(
            PipelineLineageReport report,
            CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class ThrowingLineageSinkConstructor : ILineageSink
    {
        public ThrowingLineageSinkConstructor()
        {
            throw new IOException("Disk failure during init");
        }

        public Task RecordAsync(
            LineageRecord record,
            CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class TypeWithoutParameterlessConstructor
    {
        public TypeWithoutParameterlessConstructor(string _)
        {
        }
    }

    private sealed class OptionalLoggerPipelineLineageSink(ILogger<OptionalLoggerPipelineLineageSink>? logger = null, string name = "default")
        : IPipelineLineageSink
    {
        public ILogger? Logger { get; } = logger;

        public string Name { get; } = name;

        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class LoggerFactoryLineageSink(ILoggerFactory loggerFactory, ILogger logger) : ILineageSink
    {
        public ILoggerFactory LoggerFactory { get; } = loggerFactory;

        public ILogger Logger { get; } = logger;

        public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class RequiredArgumentPipelineLineageSink(string path) : IPipelineLineageSink
    {
        public string Path { get; } = path;

        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class MultiConstructorPipelineLineageSink : IPipelineLineageSink
    {
        public MultiConstructorPipelineLineageSink()
        {
        }

        public MultiConstructorPipelineLineageSink(ILogger<MultiConstructorPipelineLineageSink> logger)
        {
            UsedLoggerConstructor = logger is not null;
        }

        public MultiConstructorPipelineLineageSink(ILogger<MultiConstructorPipelineLineageSink> logger, string path)
        {
            throw new InvalidOperationException($"Unsatisfiable constructor must not be chosen ({logger}, {path}).");
        }

        public bool UsedLoggerConstructor { get; }

        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    #endregion
}
