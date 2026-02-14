using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.ChangeDetection;
using Streamly.Core.Runtime.Channel;
using Streamly.Core.Runtime.Configuration;
using Streamly.Core.Runtime.Context;
using Streamly.Core.Runtime.Leadership;
using Streamly.Core.Runtime.Publishing;
using Streamly.Core.Runtime.Registration;
using Streamly.Core.Runtime.RequestManagement;
using Streamly.Infrastructure.Interfaces;
using Streamly.Infrastructure.Redis;

namespace Streamly.Core.Runtime;

public static class StreamlyServiceCollectionExtensions
{
    /// <summary>
    /// Add Streamly library with all required services
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="configuration">Application configuration (to read Streamly:Redis and Streamly:LeaderElection sections)</param>
    /// <param name="configure">Configure handlers</param>
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
        
        // 1. Bind configuration from appsettings.json
        
        // Bind state sync configuration
        services.Configure<StateSyncOptions>(
            configuration.GetSection(StateSyncOptions.SectionName));

        // Register default request identity provider
        services.TryAddSingleton(typeof(IRequestIdentityProvider<>), 
            typeof(DefaultRequestIdentityProvider<>));

        services.AddSingleton(typeof(IResponsePublisher<,>),
            typeof(ResponsePublisher<,>));

        services.AddSingleton(typeof(IStreamingContextFactory<,>),
            typeof(StreamingContextFactory<,>));
        
        // Register request registry (scoped per stream type)
        services.TryAddSingleton(typeof(IRequestRegistry<,>), 
            typeof(RequestRegistry<,>));

        // Register request manager (scoped per stream type)
        services.TryAddSingleton(typeof(IRequestManager<,>), 
            typeof(RequestManager<,>));
        
        services.Configure<StreamlyRuntimeOptions>(
            configuration.GetSection(StreamlyRuntimeOptions.SectionName));
        
        services.Configure<RedisConnectionOptions>(
            configuration.GetSection(RedisConnectionOptions.SectionName));
        
        services.Configure<LeaderElectionOptions>(
            configuration.GetSection(LeaderElectionOptions.SectionName));
        
        // 2. Register validators
        services.AddSingleton<IValidateOptions<StreamlyRuntimeOptions>, 
            StreamlyRuntimeOptionsValidator>();
        
        // 3. Register Infrastructure layer (Redis)
        services.AddSingleton<IRedisConnectionManager, RedisConnectionManager>();
        services.AddSingleton<IMessageSerializer, MessageSerializer>();
        
        // 4. Register Runtime core services
        services.AddSingleton(options); // StreamRegistry reads from this
        services.AddSingleton<IStreamRegistry, StreamRegistry>();
        services.AddSingleton<IChannelNameResolver, ChannelNameResolver>();
        services.AddSingleton<ILeaderElectionFactory, LeaderElectionFactory>();
        
        // 5. Register all handlers
        foreach (var handler in options.Handlers)
        {
            RegisterHandler(services, handler);
        }
        
        return services;
    }
    
    private static void ValidateHandlers(StreamlyOptions options)
    {
        if (options.Handlers.Count == 0)
        {
            throw new InvalidOperationException(
                "No handlers registered. Call options.AddHandler<TRequest, TResponse, THandler>(streamName) at least once.");
        }
        
        // Check for duplicate request types
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
        
        // Check for duplicate stream names
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
        // Register handler as scoped
        services.TryAddScoped(handler.HandlerType);
        
        // Register as IStreamingRequestHandler<TRequest, TResponse>
        var interfaceType = typeof(IStreamingRequestHandler<,>)
            .MakeGenericType(handler.RequestType, handler.ResponseType);
        
        services.TryAddScoped(interfaceType, sp => 
            sp.GetRequiredService(handler.HandlerType));
        
        // Register default change detector
        var detectorType = typeof(IResponseChangeDetector<>)
            .MakeGenericType(handler.ResponseType);
        var defaultDetectorType = typeof(DefaultResponseChangeDetector<>)
            .MakeGenericType(handler.ResponseType);
        
        services.TryAddSingleton(detectorType, defaultDetectorType);
    }
}