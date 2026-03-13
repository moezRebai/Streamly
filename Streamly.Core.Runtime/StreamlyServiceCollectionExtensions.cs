using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.ChangeDetection;
using Streamly.Core.Configurations;
using Streamly.Core.Runtime.Configuration;
using Streamly.Core.Runtime.Context;
using Streamly.Core.Runtime.Leadership;
using Streamly.Core.Runtime.Publishing;
using Streamly.Core.Runtime.Registration;
using Streamly.Core.Runtime.RequestManagement;
using Streamly.Infrastructure.Interfaces;
using Streamly.Infrastructure.Nats;
using Streamly.Infrastructure.Serialization;

namespace Streamly.Core.Runtime;

public static class StreamlyServiceCollectionExtensions
{
    /// <summary>
    /// Add Streamly with NATS transport
    /// </summary>
    public static IServiceCollection AddStreamly(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<StreamlyOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(configure);

        // Build handler options
        var options = new StreamlyOptions();
        configure(options);

        // Validate handlers
        ValidateHandlers(options);

        // 1. Bind configuration sections
        services.Configure<StateSyncOptions>(
            configuration.GetSection(StateSyncOptions.SectionName));

        services.Configure<StreamlyRuntimeOptions>(
            configuration.GetSection(StreamlyRuntimeOptions.SectionName));

        services.Configure<LeaderElectionOptions>(
            configuration.GetSection(LeaderElectionOptions.SectionName));

        // 2. Register validators
        services.AddSingleton<IValidateOptions<StreamlyRuntimeOptions>,
            StreamlyRuntimeOptionsValidator>();

        services.AddSingleton<KeepaliveServiceFactory>();
        
        // 3. Register NATS Infrastructure
        services.AddNatsPublisherInfrastructure(configuration.GetSection("Streamly:Nats"));
        services.TryAddSingleton<ISubjectResolver, NatsSubjectResolver>();
        services.TryAddSingleton<IMessageSerializer, MessageSerializer>();

        // 4. Register Runtime core services (transport-agnostic)
        services.AddSingleton(options);
        services.TryAddSingleton<IStreamRegistry, StreamRegistry>();
        services.TryAddSingleton<ILeaderElectionFactory, LeaderElectionFactory>();

        // 5. Register generic runtime components
        services.TryAddSingleton(typeof(IRequestIdentityProvider<>),
            typeof(DefaultRequestIdentityProvider<>));

        services.TryAddSingleton(typeof(IRequestRegistry<,>),
            typeof(RequestRegistry<,>));

        services.TryAddSingleton(typeof(IResponsePublisher<,>),
            typeof(ResponsePublisher<,>));

        services.TryAddSingleton(typeof(IStreamingContextFactory<,>),
            typeof(StreamingContextFactory<,>));

        services.TryAddSingleton(typeof(IRequestManager<,>),
            typeof(RequestManager<,>));

        services.TryAddSingleton<IConfirmationPublisherFactory, ConfirmationPublisherFactory>();

        // 6. Register all handlers + their change detectors
        foreach (var handler in options.Handlers)
        {
            RegisterHandler(services, handler);
        }

        services.AddHostedService<StreamlyHostedService>();

        return services;
    }

    private static void ValidateHandlers(StreamlyOptions options)
    {
        if (options.Handlers.Count == 0)
        {
            throw new InvalidOperationException(
                "No handlers registered. Call options.AddHandler<TRequest, TResponse, THandler>() at least once.");
        }

        var duplicateRequestTypes = options.Handlers
            .GroupBy(h => h.RequestType)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key.Name)
            .ToList();

        if (duplicateRequestTypes.Any())
        {
            throw new InvalidOperationException(
                $"Request types registered multiple times: {string.Join(", ", duplicateRequestTypes)}");
        }

        var duplicateStreamNames = options.Handlers
            .GroupBy(h => h.StreamName)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        if (duplicateStreamNames.Any())
        {
            throw new InvalidOperationException(
                $"Stream names registered multiple times: {string.Join(", ", duplicateStreamNames)}");
        }
    }

    private static void RegisterHandler(IServiceCollection services, HandlerRegistration handler)
    {
        // ✅ Singleton - handler stays alive for the entire application lifetime
        // One instance per stream type, continuously processing requests
        services.TryAddSingleton(handler.HandlerType);

        // Register as IStreamingRequestHandler<TRequest, TResponse> (singleton)
        var interfaceType = typeof(IStreamingRequestHandler<,>)
            .MakeGenericType(handler.RequestType, handler.ResponseType);

        services.TryAddSingleton(interfaceType, sp =>
            sp.GetRequiredService(handler.HandlerType));

        // Register default change detector (already singleton - no change)
        var detectorType = typeof(IResponseChangeDetector<>)
            .MakeGenericType(handler.ResponseType);
        var defaultDetectorType = typeof(DefaultResponseChangeDetector<>)
            .MakeGenericType(handler.ResponseType);

        services.TryAddSingleton(detectorType, defaultDetectorType);
    }
}