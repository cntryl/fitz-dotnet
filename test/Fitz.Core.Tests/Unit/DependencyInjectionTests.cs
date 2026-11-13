using Cntryl.Fitz.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class DependencyInjectionTests
{
    static readonly ConnectWhenReadyOptions FastRetry = new(
        Timeout: TimeSpan.FromSeconds(5),
        Backoff: TimeSpan.FromMilliseconds(1),
        MaxBackoff: TimeSpan.FromMilliseconds(1));

    [Fact]
    public async Task ShouldConnectAndCloseClientWithHostedLifecycleGivenHostedClientWhenLifecycleRuns()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        var services = new ServiceCollection();
        services.AddFitzClient(new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            TransportFactory: _ => transport));
        await using var provider = services.BuildServiceProvider();

        // Act
        var client = provider.GetRequiredService<IFitzClient>();

        // Assert
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());

        await hostedService.StartAsync(CancellationToken.None);
        Assert.Equal(ConnectionState.Authenticated, client.State);

        await hostedService.StopAsync(CancellationToken.None);
        Assert.Equal(ConnectionState.Closed, client.State);
    }

    [Fact]
    public async Task ShouldShareOneClientAcrossConfigAndInterfaceGivenRegistration()
    {
        // Arrange
        var config = Config();
        var services = new ServiceCollection().AddFitzClient(config);
        await using var provider = services.BuildServiceProvider();

        // Act
        var concrete = provider.GetRequiredService<FitzClient>();
        var abstraction = provider.GetRequiredService<IFitzClient>();

        // Assert
        Assert.Same(concrete, abstraction);
        Assert.Same(config, provider.GetRequiredService<ClientConfig>());
        Assert.Same(config, concrete.Config);
    }

    [Fact]
    public async Task ShouldResolveEachDomainClientFromRegisteredClientGivenRegistration()
    {
        // Arrange
        var services = new ServiceCollection().AddFitzClient(Config());
        await using var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<IFitzClient>();

        // Act
        var resolved = new object[]
        {
            provider.GetRequiredService<IKvClient>(),
            provider.GetRequiredService<IQueueClient>(),
            provider.GetRequiredService<ILeaseClient>(),
            provider.GetRequiredService<INoticeClient>(),
            provider.GetRequiredService<IRpcClient>(),
            provider.GetRequiredService<IScheduleClient>(),
            provider.GetRequiredService<IStreamClient>(),
        };

        // Assert
        Assert.Equal(
            new object[] { client.Kv, client.Queue, client.Lease, client.Notice, client.Rpc, client.Schedule, client.Stream },
            resolved);
    }

    [Fact]
    public async Task ShouldKeepEarlierDomainRegistrationGivenTryAddSemantics()
    {
        // Arrange
        var replacement = new StubKvClient();
        var services = new ServiceCollection();
        services.AddSingleton<IKvClient>(replacement);
        services.AddFitzClient(Config());
        await using var provider = services.BuildServiceProvider();

        // Act
        var kv = provider.GetRequiredService<IKvClient>();

        // Assert
        Assert.Same(replacement, kv);
        Assert.NotNull(provider.GetRequiredService<IQueueClient>());
    }

    [Fact]
    public async Task ShouldDisposeProviderCleanlyGivenResolvedDomainClients()
    {
        // Arrange
        var provider = new ServiceCollection().AddFitzClient(Config()).BuildServiceProvider();
        var client = provider.GetRequiredService<IFitzClient>();
        _ = provider.GetRequiredService<IKvClient>();
        _ = provider.GetRequiredService<IQueueClient>();
        _ = provider.GetRequiredService<IStreamClient>();

        // Act
        var dispose = () => provider.DisposeAsync().AsTask();

        // Assert
        await dispose();
        Assert.Equal(ConnectionState.Closed, client.State);
    }

    [Fact]
    public void ShouldThrowGivenSecondUnkeyedRegistration()
    {
        // Arrange
        var services = new ServiceCollection().AddFitzClient(Config());

        // Act
        var second = () => services.AddFitzClient(Config());

        // Assert
        var error = Assert.Throws<InvalidOperationException>(second);
        Assert.Contains("AddKeyedFitzClient", error.Message, StringComparison.Ordinal);
        Assert.Single(services, static descriptor => descriptor.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public void ShouldThrowGivenSecondRegistrationOfSameKey()
    {
        // Arrange
        var services = new ServiceCollection().AddKeyedFitzClient("orders", Config());

        // Act
        var second = () => services.AddKeyedFitzClient("orders", Config());

        // Assert
        Assert.Throws<InvalidOperationException>(second);
        Assert.Single(services, static descriptor => descriptor.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public async Task ShouldResolveIndependentClientsGivenDistinctKeysAndUnkeyedRegistration()
    {
        // Arrange
        var orders = Config("ws://orders/ws");
        var billing = Config("ws://billing/ws");
        var services = new ServiceCollection()
            .AddKeyedFitzClient("orders", orders)
            .AddKeyedFitzClient("billing", billing)
            .AddFitzClient(Config("ws://default/ws"));
        await using var provider = services.BuildServiceProvider();

        // Act
        var ordersClient = provider.GetRequiredKeyedService<FitzClient>("orders");
        var billingClient = provider.GetRequiredKeyedService<IFitzClient>("billing");
        var defaultClient = provider.GetRequiredService<IFitzClient>();

        // Assert
        Assert.Same(orders, ordersClient.Config);
        Assert.Same(billing, provider.GetRequiredKeyedService<ClientConfig>("billing"));
        Assert.NotSame(ordersClient, billingClient);
        Assert.NotSame(ordersClient, defaultClient);
        Assert.Same(ordersClient.Kv, provider.GetRequiredKeyedService<IKvClient>("orders"));
        Assert.Same(billingClient.Queue, provider.GetRequiredKeyedService<IQueueClient>("billing"));
        Assert.NotSame(provider.GetRequiredKeyedService<IKvClient>("orders"), provider.GetRequiredService<IKvClient>());
        Assert.Equal(3, provider.GetServices<IHostedService>().Count());
        Assert.Null(provider.GetKeyedService<IFitzClient>("missing"));
    }

    [Fact]
    public async Task ShouldConnectEachKeyedClientGivenHostedLifecycle()
    {
        // Arrange
        await using var first = new TestQueuedTransport();
        await using var second = new TestQueuedTransport();
        var services = new ServiceCollection()
            .AddKeyedFitzClient("first", Config(transport: first))
            .AddKeyedFitzClient("second", Config(transport: second));
        await using var provider = services.BuildServiceProvider();

        // Act
        foreach (var hosted in provider.GetServices<IHostedService>())
        {
            await hosted.StartAsync(CancellationToken.None);
        }

        // Assert
        Assert.Equal(ConnectionState.Authenticated, provider.GetRequiredKeyedService<IFitzClient>("first").State);
        Assert.Equal(ConnectionState.Authenticated, provider.GetRequiredKeyedService<IFitzClient>("second").State);
    }

    [Fact]
    public async Task ShouldBuildConfigFromContainerOnceGivenFactoryOverload()
    {
        // Arrange
        var builds = 0;
        var services = new ServiceCollection();
        services.AddSingleton(new BrokerSettings("ws://configured/ws", "service-token"));
        services.AddFitzClient(sp =>
        {
            builds++;
            var settings = sp.GetRequiredService<BrokerSettings>();
            return new ClientConfig(
                new Uri(settings.Url),
                TokenProvider: _ => new ValueTask<string>(settings.Token));
        });
        await using var provider = services.BuildServiceProvider();

        // Act
        var client = provider.GetRequiredService<FitzClient>();
        _ = provider.GetRequiredService<ClientConfig>();
        var token = await client.Config.TokenProvider!(CancellationToken.None);

        // Assert
        Assert.Equal(new Uri("ws://configured/ws"), client.Config.Url);
        Assert.Equal("service-token", token);
        Assert.Equal(1, builds);
    }

    [Fact]
    public async Task ShouldBuildKeyedConfigFromContainerGivenKeyedFactoryOverload()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton(new BrokerSettings("ws://keyed/ws", "t"));
        services.AddKeyedFitzClient("k", sp => Config(sp.GetRequiredService<BrokerSettings>().Url));
        await using var provider = services.BuildServiceProvider();

        // Act
        var client = provider.GetRequiredKeyedService<FitzClient>("k");

        // Assert
        Assert.Equal(new Uri("ws://keyed/ws"), client.Config.Url);
    }

    [Fact]
    public async Task ShouldRetryUntilBrokerIsUpGivenDefaultStartup()
    {
        // Arrange
        var attempts = 0;
        await using var up = new TestQueuedTransport();
        var services = new ServiceCollection().AddFitzClient(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                TransportFactory: _ => ++attempts < 3 ? new RefusingTransport() : up),
            new FitzClientHostingOptions { ConnectWhenReady = FastRetry });
        await using var provider = services.BuildServiceProvider();
        var hosted = Assert.Single(provider.GetServices<IHostedService>());

        // Act
        await hosted.StartAsync(CancellationToken.None);

        // Assert
        Assert.Equal(3, attempts);
        Assert.Equal(ConnectionState.Authenticated, provider.GetRequiredService<IFitzClient>().State);
    }

    [Fact]
    public async Task ShouldFailStartupWithTimeoutGivenBrokerNeverComesUp()
    {
        // Arrange
        var services = new ServiceCollection().AddFitzClient(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                TransportFactory: _ => new RefusingTransport()),
            new FitzClientHostingOptions
            {
                ConnectWhenReady = new ConnectWhenReadyOptions(
                    Timeout: TimeSpan.FromMilliseconds(100),
                    Backoff: TimeSpan.FromMilliseconds(1),
                    MaxBackoff: TimeSpan.FromMilliseconds(1)),
            });
        await using var provider = services.BuildServiceProvider();
        var hosted = Assert.Single(provider.GetServices<IHostedService>());

        // Act
        var start = () => hosted.StartAsync(CancellationToken.None);

        // Assert
        await Assert.ThrowsAsync<TimeoutException>(start);
    }

    [Fact]
    public async Task ShouldFailFastWithoutRetryGivenConnectStartupAndBrokerDown()
    {
        // Arrange
        var attempts = 0;
        var services = new ServiceCollection().AddFitzClient(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Reconnect: new ReconnectOptions(Enabled: false),
                TransportFactory: _ =>
                {
                    attempts++;
                    return new RefusingTransport();
                }),
            new FitzClientHostingOptions { Startup = FitzClientStartupMode.Connect });
        await using var provider = services.BuildServiceProvider();
        var hosted = Assert.Single(provider.GetServices<IHostedService>());

        // Act
        var start = () => hosted.StartAsync(CancellationToken.None);

        // Assert
        await Assert.ThrowsAnyAsync<Exception>(start);
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task ShouldNotConnectGivenManualStartup()
    {
        // Arrange
        var attempts = 0;
        var services = new ServiceCollection().AddFitzClient(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                TransportFactory: _ =>
                {
                    attempts++;
                    return new RefusingTransport();
                }),
            new FitzClientHostingOptions { Startup = FitzClientStartupMode.Manual });
        await using var provider = services.BuildServiceProvider();
        var hosted = Assert.Single(provider.GetServices<IHostedService>());
        var client = provider.GetRequiredService<IFitzClient>();

        // Act
        await hosted.StartAsync(CancellationToken.None);
        await hosted.StopAsync(CancellationToken.None);

        // Assert
        Assert.Equal(0, attempts);
        Assert.Equal(ConnectionState.Closed, client.State);
    }

    [Fact]
    public async Task ShouldCloseClientGivenAlreadyCanceledStopToken()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        var services = new ServiceCollection().AddFitzClient(Config(transport: transport));
        await using var provider = services.BuildServiceProvider();
        var hosted = Assert.Single(provider.GetServices<IHostedService>());
        var client = provider.GetRequiredService<IFitzClient>();
        await hosted.StartAsync(CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        // Act
        var stop = () => hosted.StopAsync(cancellation.Token);
        await Record.ExceptionAsync(stop);

        // Assert
        Assert.Equal(ConnectionState.Closed, client.State);
    }

    [Fact]
    public void ShouldRejectInvalidArgumentsGivenRegistration()
    {
        // Arrange
        var services = new ServiceCollection();
        var invalidMode = new FitzClientHostingOptions { Startup = (FitzClientStartupMode)99 };

        // Act
        var nulls = new Action[]
        {
            () => ServiceCollectionExtensions.AddFitzClient(null!, Config()),
            () => services.AddFitzClient((ClientConfig)null!),
            () => services.AddFitzClient((Func<IServiceProvider, ClientConfig>)null!),
            () => services.AddFitzClient(Config(), null!),
            () => services.AddKeyedFitzClient(null!, Config()),
            () => services.AddKeyedFitzClient("k", (ClientConfig)null!),
        };

        // Assert
        Assert.All(nulls, static action => Assert.IsAssignableFrom<ArgumentException>(Record.Exception(action)));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddFitzClient(Config(), invalidMode));
        Assert.Throws<ArgumentException>(() => services.AddKeyedFitzClient(KeyedService.AnyKey, Config()));
        Assert.Empty(services);
    }

    [Fact]
    public void ShouldRegisterOnlyFactoriesAndInstancesGivenRegistrationToKeepPackageTrimSafe()
    {
        // Arrange
        var services = new ServiceCollection()
            .AddFitzClient(Config())
            .AddKeyedFitzClient("k", Config());

        // Act
        var reflective = services
            .Where(static descriptor => descriptor.IsKeyedService
                ? descriptor.KeyedImplementationFactory is null && descriptor.KeyedImplementationInstance is null
                : descriptor.ImplementationFactory is null && descriptor.ImplementationInstance is null)
            .ToArray();

        // Assert
        Assert.Empty(reflective);
        Assert.NotEmpty(services);
    }

    static ClientConfig Config(string url = "ws://localhost:4190/ws", TestQueuedTransport? transport = null) =>
        new(
            new Uri(url),
            TransportFactory: transport is null ? null : _ => transport);

    sealed record BrokerSettings(string Url, string Token);

    sealed class RefusingTransport : ITransport
    {
        public Uri Url { get; } = new("ws://refused");

        public Task ConnectAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            return Task.FromException(new IOException("connection refused"));
        }

        public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default) => Task.CompletedTask;

        public ValueTask<PooledFrame> ReceiveAsync(CancellationToken ct = default) =>
            new(PooledFrame.Closed);

        public Task CloseAsync(CancellationToken ct = default) => Task.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    sealed class StubKvClient : IKvClient
    {
        public Task<IKvTransaction> BeginAsync(string route, KvMode mode = KvMode.ReadWrite, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<KvSubscription> SubscribeAsync(string pattern, CancellationToken ct = default) =>
            throw new NotSupportedException();
    }
}
