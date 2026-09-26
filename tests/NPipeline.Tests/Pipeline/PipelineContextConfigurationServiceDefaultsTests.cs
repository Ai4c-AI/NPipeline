using AwesomeAssertions;
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Lineage;
using NPipeline.Observability;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Pipeline;

/// <summary>
///     <see cref="PipelineContextConfiguration.WithServiceDefaults" /> fills the services a configuration leaves unset from a
///     container, which is how a context created through DI gets the container's logging and tracing.
/// </summary>
public sealed class PipelineContextConfigurationServiceDefaultsTests
{
    [Fact]
    public void WithServiceDefaults_FillsUnsetServicesFromTheContainer()
    {
        // Arrange
        var errorHandlerFactory = A.Fake<IErrorHandlerFactory>();
        var lineageFactory = A.Fake<ILineageFactory>();
        var observabilityFactory = A.Fake<IObservabilityFactory>();
        var loggerFactory = A.Fake<ILoggerFactory>();
        var tracer = A.Fake<IPipelineTracer>();

        using var provider = new ServiceCollection()
            .AddSingleton(errorHandlerFactory)
            .AddSingleton(lineageFactory)
            .AddSingleton(observabilityFactory)
            .AddSingleton(loggerFactory)
            .AddSingleton(tracer)
            .BuildServiceProvider();

        // Act
        var config = PipelineContextConfiguration.Default.WithServiceDefaults(provider);

        // Assert
        config.ErrorHandlerFactory.Should().BeSameAs(errorHandlerFactory);
        config.LineageFactory.Should().BeSameAs(lineageFactory);
        config.ObservabilityFactory.Should().BeSameAs(observabilityFactory);
        config.LoggerFactory.Should().BeSameAs(loggerFactory);
        config.Tracer.Should().BeSameAs(tracer);
    }

    [Fact]
    public void WithServiceDefaults_KeepsServicesTheConfigurationAlreadySets()
    {
        // Arrange
        var ownLoggerFactory = A.Fake<ILoggerFactory>();
        var ownTracer = A.Fake<IPipelineTracer>();
        var ownLineageFactory = A.Fake<ILineageFactory>();

        using var provider = new ServiceCollection()
            .AddSingleton(A.Fake<ILoggerFactory>())
            .AddSingleton(A.Fake<IPipelineTracer>())
            .AddSingleton(A.Fake<ILineageFactory>())
            .BuildServiceProvider();

        var original = new PipelineContextConfiguration(LoggerFactory: ownLoggerFactory, Tracer: ownTracer, LineageFactory: ownLineageFactory);

        // Act
        var config = original.WithServiceDefaults(provider);

        // Assert
        config.LoggerFactory.Should().BeSameAs(ownLoggerFactory);
        config.Tracer.Should().BeSameAs(ownTracer);
        config.LineageFactory.Should().BeSameAs(ownLineageFactory);
    }

    [Fact]
    public void WithServiceDefaults_LeavesServicesTheContainerDoesNotRegisterUnset()
    {
        // Arrange
        using var provider = new ServiceCollection().BuildServiceProvider();
        var parameters = new Dictionary<string, object> { ["key"] = "value" };
        using var cts = new CancellationTokenSource();

        // Act
        var config = new PipelineContextConfiguration(parameters, CancellationToken: cts.Token).WithServiceDefaults(provider);

        // Assert
        config.LoggerFactory.Should().BeNull();
        config.Tracer.Should().BeNull();
        config.ErrorHandlerFactory.Should().BeNull();
        config.Parameters.Should().BeSameAs(parameters);
        config.CancellationToken.Should().Be(cts.Token);
    }

    [Fact]
    public async Task WithServiceDefaults_ContextUsesTheContainersLoggerFactoryAndTracer()
    {
        // Arrange
        var loggerFactory = A.Fake<ILoggerFactory>();
        var tracer = A.Fake<IPipelineTracer>();

        using var provider = new ServiceCollection()
            .AddSingleton(loggerFactory)
            .AddSingleton(tracer)
            .BuildServiceProvider();

        // Act
        await using var context = new PipelineContext(PipelineContextConfiguration.Default.WithServiceDefaults(provider));

        // Assert
        context.Observability.LoggerFactory.Should().BeSameAs(loggerFactory);
        context.Observability.Tracer.Should().BeSameAs(tracer);
    }

    [Fact]
    public void WithServiceDefaults_WithNullServiceProvider_Throws()
    {
        // Act
        var act = () => PipelineContextConfiguration.Default.WithServiceDefaults(null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }
}
