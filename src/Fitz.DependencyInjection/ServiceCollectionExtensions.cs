using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Cntryl.Fitz.DependencyInjection;

/// <summary>
/// Registers a Fitz client with a dependency injection container.
/// </summary>
/// <remarks>
/// <para>
/// Each registration adds the <see cref="FitzClient"/>, its <see cref="IFitzClient"/> view, a client for
/// every domain (<see cref="IKvClient"/>, <see cref="IQueueClient"/>, <see cref="ILeaseClient"/>,
/// <see cref="INoticeClient"/>, <see cref="IRpcClient"/>, <see cref="IScheduleClient"/>,
/// <see cref="IStreamClient"/>) resolved from that client, and a hosted service that connects at
/// host startup and closes at shutdown. Domain clients are registered with try-add semantics, so a
/// registration you made earlier, such as a test double, is left in place.
/// </para>
/// <para>
/// A second unkeyed call, or a second call for the same key, throws
/// <see cref="InvalidOperationException"/> rather than silently ignoring or doubling the
/// registration. Use the keyed methods to talk to more than one broker.
/// </para>
/// <para>
/// Every registration is an explicit factory, so nothing is activated by reflection and the
/// package stays trim and Native AOT safe.
/// </para>
/// <para>
/// The container owns and disposes what it creates. Disposing the provider closes the client.
/// </para>
/// </remarks>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a client for <paramref name="config"/> and a hosted lifecycle that connects with
    /// <c>ConnectWhenReadyAsync</c> during host startup and closes during shutdown.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="config">Configuration for the client, registered as a singleton.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">A Fitz client is already registered.</exception>
    /// <remarks>
    /// Startup now retries a broker that is not up yet (up to the client's request timeout) instead
    /// of failing host start on the first refused connection; pass
    /// <see cref="FitzClientHostingOptions"/> with <see cref="FitzClientStartupMode.Connect"/> to
    /// fail fast as earlier versions did. Outside Generic Host, call <c>ConnectAsync</c> or
    /// <c>ConnectWhenReadyAsync</c> yourself.
    /// </remarks>
    public static IServiceCollection AddFitzClient(this IServiceCollection services, ClientConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        return services.AddFitzClient(config, new FitzClientHostingOptions());
    }

    /// <summary>
    /// Registers a client for <paramref name="config"/> and a hosted lifecycle configured by
    /// <paramref name="hosting"/>.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="config">Configuration for the client, registered as a singleton.</param>
    /// <param name="hosting">Startup behavior of the hosted lifecycle.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">A Fitz client is already registered.</exception>
    public static IServiceCollection AddFitzClient(
        this IServiceCollection services,
        ClientConfig config,
        FitzClientHostingOptions hosting)
    {
        ArgumentNullException.ThrowIfNull(config);
        return services.AddFitzClient(_ => config, hosting);
    }

    /// <summary>
    /// Registers a client whose configuration is built from the container, so the endpoint or
    /// token provider can come from options, secrets, or other registered services.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configFactory">
    /// Builds the configuration the first time the client is needed. It runs once; the result is a
    /// singleton <see cref="ClientConfig"/> registration.
    /// </param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">A Fitz client is already registered.</exception>
    public static IServiceCollection AddFitzClient(
        this IServiceCollection services,
        Func<IServiceProvider, ClientConfig> configFactory) =>
        services.AddFitzClient(configFactory, new FitzClientHostingOptions());

    /// <summary>
    /// Registers a client whose configuration is built from the container, with an explicit
    /// startup behavior.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configFactory">Builds the configuration the first time the client is needed.</param>
    /// <param name="hosting">Startup behavior of the hosted lifecycle.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">A Fitz client is already registered.</exception>
    public static IServiceCollection AddFitzClient(
        this IServiceCollection services,
        Func<IServiceProvider, ClientConfig> configFactory,
        FitzClientHostingOptions hosting)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configFactory);
        ValidateHosting(hosting);
        if (services.Any(static descriptor => !descriptor.IsKeyedService && descriptor.ServiceType == typeof(FitzClient)))
        {
            throw new InvalidOperationException(
                "A Fitz client is already registered. Call AddFitzClient once, or use AddKeyedFitzClient " +
                "to register an additional client for another broker.");
        }

        // Every registration supplies an explicit factory: the type-based overloads would
        // have the container select and invoke constructors reflectively, which also forces
        // the trimmer to preserve them.
        services.AddSingleton(configFactory);
        services.AddSingleton(static sp => new FitzClient(sp.GetRequiredService<ClientConfig>()));
        services.AddSingleton<IFitzClient>(static sp => sp.GetRequiredService<FitzClient>());
        services.TryAddSingleton<IKvClient>(static sp => sp.GetRequiredService<IFitzClient>().Kv);
        services.TryAddSingleton<IQueueClient>(static sp => sp.GetRequiredService<IFitzClient>().Queue);
        services.TryAddSingleton<ILeaseClient>(static sp => sp.GetRequiredService<IFitzClient>().Lease);
        services.TryAddSingleton<INoticeClient>(static sp => sp.GetRequiredService<IFitzClient>().Notice);
        services.TryAddSingleton<IRpcClient>(static sp => sp.GetRequiredService<IFitzClient>().Rpc);
        services.TryAddSingleton<IScheduleClient>(static sp => sp.GetRequiredService<IFitzClient>().Schedule);
        services.TryAddSingleton<IStreamClient>(static sp => sp.GetRequiredService<IFitzClient>().Stream);
        services.AddSingleton<IHostedService>(sp => new FitzClientHostedService(
            sp.GetRequiredService<FitzClient>(), hosting));
        return services;
    }

    /// <summary>
    /// Registers a client for one of several brokers under <paramref name="serviceKey"/>, using
    /// .NET keyed services, and a hosted lifecycle that connects it with
    /// <c>ConnectWhenReadyAsync</c> during host startup.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="serviceKey">The key to resolve this client by, for example with <c>[FromKeyedServices]</c>.</param>
    /// <param name="config">Configuration for the client, registered as a keyed singleton.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="serviceKey"/> is <see cref="KeyedService.AnyKey"/>.</exception>
    /// <exception cref="InvalidOperationException">A Fitz client is already registered under the key.</exception>
    /// <remarks>
    /// The keyed client, its <see cref="ClientConfig"/>, and each domain client are registered under
    /// <paramref name="serviceKey"/>. Unkeyed registrations from <c>AddFitzClient</c> are not
    /// affected and the two may coexist.
    /// </remarks>
    public static IServiceCollection AddKeyedFitzClient(
        this IServiceCollection services,
        object serviceKey,
        ClientConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        return services.AddKeyedFitzClient(serviceKey, config, new FitzClientHostingOptions());
    }

    /// <summary>
    /// Registers a keyed client with an explicit startup behavior.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="serviceKey">The key to resolve this client by.</param>
    /// <param name="config">Configuration for the client, registered as a keyed singleton.</param>
    /// <param name="hosting">Startup behavior of the hosted lifecycle.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="serviceKey"/> is <see cref="KeyedService.AnyKey"/>.</exception>
    /// <exception cref="InvalidOperationException">A Fitz client is already registered under the key.</exception>
    public static IServiceCollection AddKeyedFitzClient(
        this IServiceCollection services,
        object serviceKey,
        ClientConfig config,
        FitzClientHostingOptions hosting)
    {
        ArgumentNullException.ThrowIfNull(config);
        return services.AddKeyedFitzClient(serviceKey, _ => config, hosting);
    }

    /// <summary>
    /// Registers a keyed client whose configuration is built from the container.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="serviceKey">The key to resolve this client by.</param>
    /// <param name="configFactory">Builds the configuration the first time the client is needed.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="serviceKey"/> is <see cref="KeyedService.AnyKey"/>.</exception>
    /// <exception cref="InvalidOperationException">A Fitz client is already registered under the key.</exception>
    public static IServiceCollection AddKeyedFitzClient(
        this IServiceCollection services,
        object serviceKey,
        Func<IServiceProvider, ClientConfig> configFactory) =>
        services.AddKeyedFitzClient(serviceKey, configFactory, new FitzClientHostingOptions());

    /// <summary>
    /// Registers a keyed client whose configuration is built from the container, with an explicit
    /// startup behavior.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="serviceKey">The key to resolve this client by.</param>
    /// <param name="configFactory">Builds the configuration the first time the client is needed.</param>
    /// <param name="hosting">Startup behavior of the hosted lifecycle.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="serviceKey"/> is <see cref="KeyedService.AnyKey"/>.</exception>
    /// <exception cref="InvalidOperationException">A Fitz client is already registered under the key.</exception>
    public static IServiceCollection AddKeyedFitzClient(
        this IServiceCollection services,
        object serviceKey,
        Func<IServiceProvider, ClientConfig> configFactory,
        FitzClientHostingOptions hosting)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(serviceKey);
        ArgumentNullException.ThrowIfNull(configFactory);
        ValidateHosting(hosting);
        if (ReferenceEquals(serviceKey, KeyedService.AnyKey))
        {
            throw new ArgumentException("KeyedService.AnyKey cannot identify a single client.", nameof(serviceKey));
        }
        if (services.Any(descriptor =>
            descriptor.IsKeyedService &&
            descriptor.ServiceType == typeof(FitzClient) &&
            Equals(descriptor.ServiceKey, serviceKey)))
        {
            throw new InvalidOperationException(
                $"A Fitz client is already registered under the key '{serviceKey}'. Use a different key.");
        }

        services.AddKeyedSingleton(serviceKey, (sp, _) => configFactory(sp));
        services.AddKeyedSingleton(
            serviceKey,
            static (sp, key) => new FitzClient(sp.GetRequiredKeyedService<ClientConfig>(key)));
        services.AddKeyedSingleton<IFitzClient>(
            serviceKey,
            static (sp, key) => sp.GetRequiredKeyedService<FitzClient>(key));
        services.TryAddKeyedSingleton<IKvClient>(
            serviceKey,
            static (sp, key) => sp.GetRequiredKeyedService<IFitzClient>(key).Kv);
        services.TryAddKeyedSingleton<IQueueClient>(
            serviceKey,
            static (sp, key) => sp.GetRequiredKeyedService<IFitzClient>(key).Queue);
        services.TryAddKeyedSingleton<ILeaseClient>(
            serviceKey,
            static (sp, key) => sp.GetRequiredKeyedService<IFitzClient>(key).Lease);
        services.TryAddKeyedSingleton<INoticeClient>(
            serviceKey,
            static (sp, key) => sp.GetRequiredKeyedService<IFitzClient>(key).Notice);
        services.TryAddKeyedSingleton<IRpcClient>(
            serviceKey,
            static (sp, key) => sp.GetRequiredKeyedService<IFitzClient>(key).Rpc);
        services.TryAddKeyedSingleton<IScheduleClient>(
            serviceKey,
            static (sp, key) => sp.GetRequiredKeyedService<IFitzClient>(key).Schedule);
        services.TryAddKeyedSingleton<IStreamClient>(
            serviceKey,
            static (sp, key) => sp.GetRequiredKeyedService<IFitzClient>(key).Stream);
        services.AddSingleton<IHostedService>(sp => new FitzClientHostedService(
            sp.GetRequiredKeyedService<FitzClient>(serviceKey), hosting));
        return services;
    }

    static void ValidateHosting(FitzClientHostingOptions hosting)
    {
        ArgumentNullException.ThrowIfNull(hosting);
        if (hosting.Startup is not (FitzClientStartupMode.ConnectWhenReady or FitzClientStartupMode.Connect or FitzClientStartupMode.Manual))
        {
            throw new ArgumentOutOfRangeException(nameof(hosting), hosting.Startup, "Unknown Fitz client startup mode.");
        }
    }

    sealed class FitzClientHostedService(FitzClient client, FitzClientHostingOptions hosting) : IHostedService
    {
        public Task StartAsync(CancellationToken ct) => hosting.Startup switch
        {
            FitzClientStartupMode.ConnectWhenReady => client.ConnectWhenReadyAsync(hosting.ConnectWhenReady, ct),
            FitzClientStartupMode.Connect => client.ConnectAsync(ct),
            _ => Task.CompletedTask,
        };

        // CloseAsync throws before doing any work when its token is already canceled, which is
        // exactly the state of a host whose shutdown timeout expired. Close regardless and let
        // the token bound only how long the host waits.
        public Task StopAsync(CancellationToken ct) => client.CloseAsync(CancellationToken.None).WaitAsync(ct);
    }
}
