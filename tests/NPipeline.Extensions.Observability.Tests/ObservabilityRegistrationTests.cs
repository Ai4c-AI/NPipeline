using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Execution;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Observability;
using NPipeline.Observability.DependencyInjection;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Registering observability more than once, or on top of the app's own registrations, keeps what was there first,
///     and every registered execution observer is notified.
/// </summary>
public sealed class ObservabilityRegistrationTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AddNPipelineObservability_ReplacesOnlyTheNullSurface(bool coreFirst)
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        if (coreFirst)
            _ = services.AddNPipeline().AddNPipelineObservability();
        else
            _ = services.AddNPipelineObservability().AddNPipeline();

        // Assert
        using var scope = services.BuildServiceProvider().CreateScope();
        _ = Assert.IsType<ObservabilitySurface>(scope.ServiceProvider.GetRequiredService<IObservabilitySurface>());
        _ = Assert.Single(services, d => d.ServiceType == typeof(IObservabilitySurface));
    }

    [Fact]
    public void AddNPipelineObservability_KeepsTheAppsSurface()
    {
        // Arrange
        var services = new ServiceCollection();
        var appSurface = A.Fake<IObservabilitySurface>();
        _ = services.AddNPipeline();
        _ = services.AddScoped<IObservabilitySurface>(_ => appSurface);

        // Act
        _ = services.AddNPipelineObservability();

        // Assert
        using var scope = services.BuildServiceProvider().CreateScope();
        Assert.Same(appSurface, scope.ServiceProvider.GetRequiredService<IObservabilitySurface>());
    }

    [Fact]
    public void AddNPipelineObservability_CalledTwice_KeepsTheFirstOptionsAndOneMetricsObserver()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        _ = services.AddNPipelineObservability(new ObservabilityExtensionOptions { AutoObserveAllNodes = true });
        _ = services.AddNPipelineObservability(ObservabilityExtensionOptions.Default);

        // Assert
        using var scope = services.BuildServiceProvider().CreateScope();
        Assert.True(scope.ServiceProvider.GetRequiredService<ObservabilityExtensionOptions>().AutoObserveAllNodes);
        _ = Assert.Single(scope.ServiceProvider.GetServices<IExecutionObserver>());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ConfigureNPipelineObservability_ChangesTheRegisteredOptions_InEitherOrder(bool configureFirst)
    {
        // Arrange
        var services = new ServiceCollection();
        var appOptions = new ObservabilityExtensionOptions { AutoObserveAllNodes = true };

        // Act
        if (configureFirst)
        {
            _ = services.ConfigureNPipelineObservability(o => o with { EnableMemoryMetrics = true });
            _ = services.AddNPipelineObservability(appOptions);
        }
        else
        {
            _ = services.AddNPipelineObservability(appOptions);
            _ = services.ConfigureNPipelineObservability(o => o with { EnableMemoryMetrics = true });
        }

        // Assert - the app's AutoObserveAllNodes survives, and memory metrics are switched on.
        var options = services.BuildServiceProvider().GetRequiredService<ObservabilityExtensionOptions>();
        Assert.True(options.AutoObserveAllNodes);
        Assert.True(options.EnableMemoryMetrics);
    }

    [Fact]
    public void ConfigureNPipelineObservability_AppliesChangesInRegistrationOrder()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();

        // Act
        _ = services.ConfigureNPipelineObservability(o => o with { EnableMemoryMetrics = true });
        _ = services.ConfigureNPipelineObservability(o => o with { EnableMemoryMetrics = false, AutoObserveAllNodes = true });

        // Assert
        var options = services.BuildServiceProvider().GetRequiredService<ObservabilityExtensionOptions>();
        Assert.False(options.EnableMemoryMetrics);
        Assert.True(options.AutoObserveAllNodes);
    }

    [Fact]
    public void AppObserverRegisteredFirst_MetricsObserverIsStillAdded_AndBothAreAttachedToTheContext()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipeline();
        _ = services.AddScoped<IExecutionObserver, AppObserver>();
        _ = services.AddNPipelineObservability();

        using var scope = services.BuildServiceProvider().CreateScope();

        // Act
        var fromExtension = scope.ServiceProvider.CreatePipelineContext();
        var fromFactory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>().Create();

        // Assert
        foreach (var context in new[] { fromExtension, fromFactory })
        {
            var composite = Assert.IsType<CompositeExecutionObserver>(context.Observability.ExecutionObserver);
            Assert.Collection(composite.Observers,
                observer => Assert.IsType<AppObserver>(observer),
                observer => Assert.IsType<MetricsCollectingExecutionObserver>(observer));
        }
    }

    [Fact]
    public void SingleObserver_IsAttachedDirectly()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipeline().AddNPipelineObservability();
        using var scope = services.BuildServiceProvider().CreateScope();

        // Act
        var context = scope.ServiceProvider.CreatePipelineContext();

        // Assert
        _ = Assert.IsType<MetricsCollectingExecutionObserver>(context.Observability.ExecutionObserver);
    }

    private sealed class AppObserver : IExecutionObserver
    {
        public void OnNodeStarted(NodeExecutionStarted e)
        {
        }

        public void OnNodeCompleted(NodeExecutionCompleted e)
        {
        }

        public void OnRetry(NodeRetryEvent e)
        {
        }

        public void OnDrop(QueueDropEvent e)
        {
        }

        public void OnQueueMetrics(QueueMetricsEvent e)
        {
        }
    }
}
