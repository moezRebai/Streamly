using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Runtime.Channel;
using Streamly.Core.Runtime.Registration;
using Streamly.Infrastructure.Interfaces;
using Streamly.Infrastructure.Redis;
using Streamly.Subscriber.Configuration;
using Streamly.Subscriber.Internal;

namespace Streamly.Subscriber;

/// <summary>
/// Extension methods for registering subscriber services
///
/// Usage (client application):
///   services.AddStreamlySubscriber(configuration, options =>
///   {
///       options.AddSubscriber{SpotRequest, SpotPrice}("SpotPricer");
///       options.AddSubscriber{FxSwapRequest, FxSwapPrice}("FxSwapPricer");
///   });
///
///   // Then in user code:
///   public class TradingApp
///   {
///       private readonly IStreamingSubscriber{SpotRequest, SpotPrice} _subscriber;
///
///       public void Start()
///       {
///           var stream = _subscriber.Stream(
///               new SpotRequest { CurrencyPair = "EUR/USD" },
///               StreamBehavior.Live);
///
///           stream.Subscribe(onNext: price => Console.WriteLine(price.Rate));
///       }
///   }
/// </summary>
public static class SubscriberServiceCollectionExtensions
{
    /// <summary>
    /// Register Streamly subscriber services
    /// Call once in Program.cs or Startup.cs
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

        // 2. Register Redis infrastructure using correct overload
        //    TryAdd inside AddRedisInfrastructure = safe to call multiple times
        //    Redis connection string comes from IConfiguration binding
        services.AddRedisInfrastructure(options =>
            configuration
                .GetSection(RedisConnectionOptions.SectionName)
                .Bind(options));                            // ← Bind from IConfiguration

        // 3. Register channel resolver
        services.TryAddSingleton<IChannelNameResolver, ChannelNameResolver>();

        // 4. Apply subscriber registrations
        var registrationOptions = new SubscriberRegistrationOptions(services);
        configure(registrationOptions);

        return services;
    }
}

/// <summary>
/// Fluent registration options for subscriber streams
/// </summary>
public class SubscriberRegistrationOptions
{
    private readonly IServiceCollection _services;

    internal SubscriberRegistrationOptions(IServiceCollection services)
    {
        _services = services;
    }

    /// <summary>
    /// Register a subscriber for a specific stream type
    ///
    /// Usage:
    ///   options.AddSubscriber{SpotRequest, SpotPrice}("SpotPricer");
    /// </summary>
    // In SubscriberServiceCollectionExtensions.cs
    public SubscriberRegistrationOptions AddSubscriber<TRequest, TResponse>(string streamName)
    {
        _services.TryAddSingleton<IStreamingSubscriber<TRequest, TResponse>>(sp =>
        {
            // Wire everything internally - DI extension has access to internals
            var redis = sp.GetRequiredService<IRedisConnectionManager>();
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var channelResolver = sp.GetRequiredService<IChannelNameResolver>();
            var streamRegistry = sp.GetRequiredService<IStreamRegistry>();
            var options = sp.GetRequiredService<IOptions<SubscriberOptions>>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();

            // Create SubscriptionManager (internal)
            var manager = new SubscriptionManager<TRequest, TResponse>(
                streamName,
                redis,
                serializer,
                channelResolver,
                options,
                loggerFactory.CreateLogger<SubscriptionManager<TRequest, TResponse>>());

            // Create StreamingSubscriber (internal) behind public interface
            return new StreamingSubscriber<TRequest, TResponse>(
                manager,
                streamRegistry,
                redis,
                serializer,
                channelResolver,
                options,
                loggerFactory.CreateLogger<StreamingSubscriber<TRequest, TResponse>>());
        });

        return this;
    }
}

/// <summary>
/// Stream registration info for subscriber side
/// </summary>
internal record SubscriberStreamRegistration(Type RequestType, string StreamName);
