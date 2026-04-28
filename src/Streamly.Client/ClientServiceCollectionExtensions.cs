using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;
using Streamly.Infrastructure.Interfaces;
using Streamly.Infrastructure.Nats;
using Streamly.Infrastructure.Serialization;

namespace Streamly.Client;

public static class ClientServiceCollectionExtensions
{
    /// <summary>
    /// Add Streamly client-side components with NATS transport.
    /// </summary>
    public static IServiceCollection AddStreamlyClient(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<ClientRegistrationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(configure);

        // 1. Bind subscriber configuration
        services.Configure<StreamlySettings>(
            configuration.GetSection(StreamlySettings.SectionName));

        services.AddSingleton<IValidateOptions<StreamlySettings>,
            StreamlyOptionsValidator>();

        // Monitoring null default — replaced by AddStreamlyMonitoring()
        services.TryAddSingleton<IStreamlyMetricsCollector>(NullMetricsCollector.Instance);

        // 2. Register NATS Infrastructure
        services.AddNatsSubscriberInfrastructure(configuration.GetSection("Streamly:Nats"));
        services.TryAddSingleton<ISubjectResolver, NatsSubjectResolver>();
        services.TryAddSingleton<IMessageSerializer, DefaultMessageSerializer>();

        // 3. Apply per-stream subscriber registrations
        var registrationOptions = new ClientRegistrationOptions(services);
        configure(registrationOptions);

        return services;
    }
}
