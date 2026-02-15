using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Runtime.Channel;
using Streamly.Infrastructure.Interfaces;
using Streamly.Infrastructure.Redis;
using Streamly.Subscriber.Configuration;
using Streamly.Subscriber.Internal;

namespace Streamly.Subscriber;

public static class SubscriberServiceCollectionExtensions
{
    public static IServiceCollection AddStreamlySubscriber(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<SubscriberRegistrationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(configure);

        // 1. Bind configuration
        services.Configure<SubscriberOptions>(
            configuration.GetSection(SubscriberOptions.SectionName));

        // 2. Register Infrastructure (safe to call multiple times via TryAdd)
        services.TryAddSingleton<IMessageSerializer, MessageSerializer>();
        services.TryAddSingleton<IRedisConnectionManager, RedisConnectionManager>();

        services.Configure<RedisConnectionOptions>(
            configuration.GetSection(RedisConnectionOptions.SectionName));

        // 3. Register ChannelNameResolver - subscriber needs it to build channel names
        //    No IStreamRegistry needed - streamName passed directly at registration
        services.TryAddSingleton<IChannelNameResolver, ChannelNameResolver>();

        // 4. Apply per-stream subscriber registrations
        var registrationOptions = new SubscriberRegistrationOptions(services);
        configure(registrationOptions);

        return services;
    }
}

public class SubscriberRegistrationOptions
{
    private readonly IServiceCollection _services;

    internal SubscriberRegistrationOptions(IServiceCollection services)
    {
        _services = services;
    }

    /// <summary>
    /// Register a subscriber for a stream type.
    /// streamName must match what the publisher registered (e.g., "SpotPricer")
    /// </summary>
    public SubscriberRegistrationOptions AddSubscriber<TRequest, TResponse>(string streamName)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));

        // Register SubscriptionManager (singleton per TRequest/TResponse)
        // streamName captured in closure - no IStreamRegistry needed
        _services.TryAddSingleton<SubscriptionManager<TRequest, TResponse>>(sp =>
        {
            var redis = sp.GetRequiredService<IRedisConnectionManager>();
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var channelResolver = sp.GetRequiredService<IChannelNameResolver>();
            var options = sp.GetRequiredService<IOptions<SubscriberOptions>>();
            var logger = sp.GetRequiredService<ILogger<SubscriptionManager<TRequest, TResponse>>>();

            return new SubscriptionManager<TRequest, TResponse>(
                streamName,       // ← passed directly, no registry lookup
                redis,
                serializer,
                channelResolver,
                options,
                logger);
        });

        // Register StreamingSubscriber behind public interface (singleton per TRequest/TResponse)
        // All deps are resolved from DI except streamName (captured in closure)
        _services.TryAddSingleton<IStreamingSubscriber<TRequest, TResponse>>(sp =>
        {
            var manager = sp.GetRequiredService<SubscriptionManager<TRequest, TResponse>>();
            var redis = sp.GetRequiredService<IRedisConnectionManager>();
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var channelResolver = sp.GetRequiredService<IChannelNameResolver>();
            var options = sp.GetRequiredService<IOptions<SubscriberOptions>>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();

            return new StreamingSubscriber<TRequest, TResponse>(
                streamName,       // ← passed directly, no registry lookup
                manager,
                redis,
                serializer,
                channelResolver,
                options,
                loggerFactory.CreateLogger<StreamingSubscriber<TRequest, TResponse>>());
        });

        return this;
    }
}