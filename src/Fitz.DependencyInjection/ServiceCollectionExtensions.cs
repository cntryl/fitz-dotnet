using Cntryl.Fitz.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Cntryl.Fitz.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFitzClient(
        this IServiceCollection services,
        ClientConfig config)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(config);

        services.AddSingleton(config);
        services.AddSingleton<Client>();
        services.AddSingleton<IClient>(sp => sp.GetRequiredService<Client>());
        services.AddSingleton<IHostedService, FitzClientHostedService>();
        return services;
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "The hosting container constructs this internal lifecycle service.")]
    sealed class FitzClientHostedService(Client client) : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) => client.ConnectAsync(cancellationToken);

        public Task StopAsync(CancellationToken cancellationToken) => client.CloseAsync(cancellationToken);
    }
}
