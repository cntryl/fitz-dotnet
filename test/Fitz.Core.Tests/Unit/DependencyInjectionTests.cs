using Cntryl.Fitz.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class DependencyInjectionTests
{
    [Fact]
    public async Task ShouldConnectAndCloseClientWithHostedLifecycleGivenHostedClientWhenLifecycleRuns()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        var services = new ServiceCollection();
        services.AddFitzClient(new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            AuthSettleDelay: TimeSpan.Zero,
            TransportFactory: _ => transport));
        await using var provider = services.BuildServiceProvider();

        // Act
        var client = provider.GetRequiredService<IClient>();

        // Assert
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());

        await hostedService.StartAsync(CancellationToken.None);
        Assert.Equal(ConnectionState.Authenticated, client.State);

        await hostedService.StopAsync(CancellationToken.None);
        Assert.Equal(ConnectionState.Closed, client.State);
    }
}
