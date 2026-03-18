using Cntryl.Fitz.Abstractions;
using Microsoft.Extensions.DependencyInjection;

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
        return services;
    }
}