using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Streamly.Core.Abstractions;
using Streamly.Infrastructure.Interfaces;
using Streamly.Infrastructure.Nats;
using Streamly.Infrastructure.Serialization;
using Streamly.Subscriber.Configuration;

namespace Streamly.Subscriber;

public static class SubscriberServiceCollectionExtensions
{
    /// <summary>
    /// Add Streamly Subscriber with NATS transport
    /// </summary>
    public static IServiceCollection AddStreamlySubscriber(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<SubscriberRegistrationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(configure);

        // 1. Bind subscriber configuration
        services.Configure<SubscriberOptions>(
            configuration.GetSection(SubscriberOptions.SectionName));

        // 2. Register NATS Infrastructure
        services.AddNatsSubscriberInfrastructure(configuration.GetSection("Streamly:Nats"));
        services.TryAddSingleton<ISubjectResolver, NatsSubjectResolver>();
        services.TryAddSingleton<IMessageSerializer, MessageSerializer>();

        // 3. Apply per-stream subscriber registrations
        var registrationOptions = new SubscriberRegistrationOptions(services);
        configure(registrationOptions);

        return services;
    }
}