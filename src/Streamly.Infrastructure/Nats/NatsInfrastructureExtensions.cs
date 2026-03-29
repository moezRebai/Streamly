using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Streamly.Core.Abstractions;

namespace Streamly.Infrastructure.Nats;

public static class NatsInfrastructureExtensions
{
    /// <summary>
    ///     Register NATS infrastructure for PUBLISHER/RUNTIME services (with leader election)
    /// </summary>
    public static IServiceCollection AddNatsPublisherInfrastructure(
        this IServiceCollection services,
        IConfigurationSection configSection)
    {
        // Bind options
        services.Configure<NatsConnectionOptions>(configSection);

        // Register connection manager
        services.TryAddSingleton<NatsConnectionManager>();
        services.TryAddSingleton<IStreamingTransport>(sp =>
            sp.GetRequiredService<NatsConnectionManager>());

        // ✅ Register leader election (ONLY for publishers)
        //services.TryAddSingleton<ILeaderElection, NatsLeaderElection>();

        // Register startup service
        services.AddHostedService<NatsStartupService>();
        return services;
    }

    /// <summary>
    ///     Register NATS infrastructure for SUBSCRIBER services (NO leader election)
    /// </summary>
    public static IServiceCollection AddNatsSubscriberInfrastructure(
        this IServiceCollection services,
        IConfigurationSection configSection)
    {
        // Bind options
        services.Configure<NatsConnectionOptions>(configSection);

        // Register connection manager
        services.TryAddSingleton<NatsConnectionManager>();
        services.TryAddSingleton<IStreamingTransport>(sp =>
            sp.GetRequiredService<NatsConnectionManager>());

        // Register startup service
        services.AddHostedService<NatsStartupService>();
        return services;
    }
}