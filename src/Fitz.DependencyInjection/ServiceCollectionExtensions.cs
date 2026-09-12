using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Cntryl.Fitz.DependencyInjection;

/// <summary>
/// Registers a Fitz client with a dependency injection container.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="Client"/>, <see cref="IClient"/>, and a hosted lifecycle that
    /// connects during host startup and closes during shutdown.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="config">Configuration for the client, registered as a singleton.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    /// <remarks>
    /// Outside Generic Host, call <c>ConnectAsync</c> or <c>ConnectWhenReadyAsync</c> yourself.
    /// </remarks>
    public static IServiceCollection AddFitzClient(
        this IServiceCollection services,
        ClientConfig config)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(config);

        // Every registration supplies an explicit factory: the type-based overloads would
        // have the container select and invoke constructors reflectively, which also forces
        // the trimmer to preserve them.
        services.AddSingleton(config);
        services.AddSingleton(static sp => new Client(sp.GetRequiredService<ClientConfig>()));
        services.AddSingleton<IClient>(static sp => sp.GetRequiredService<Client>());
        services.AddSingleton<IHostedService>(static sp => new FitzClientHostedService(sp.GetRequiredService<Client>()));
        return services;
    }

    sealed class FitzClientHostedService(Client client) : IHostedService
    {
        public Task StartAsync(CancellationToken ct) => client.ConnectAsync(ct);

        public Task StopAsync(CancellationToken ct) => client.CloseAsync(ct);
    }
}
