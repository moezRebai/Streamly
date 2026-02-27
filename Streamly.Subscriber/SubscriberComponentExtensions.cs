using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Infrastructure.Interfaces;
using Streamly.Subscriber.Configuration;
using Streamly.Subscriber.Internal;

namespace Streamly.Subscriber;

public static class SubscriberComponentExtensions
{
    /// <summary>
    /// Add subscriber components WITHOUT infrastructure registration.
    /// Use this when infrastructure is already registered (e.g., by AddStreamly).
    /// </summary>
    public static IServiceCollection AddSubscriberComponents<TRequest, TResponse>(
        this IServiceCollection services,
        string streamName)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));

        // Register SubscriptionManager (singleton per TRequest/TResponse)
        services.TryAddSingleton<SubscriptionManager<TRequest, TResponse>>(sp =>
        {
            var transport = sp.GetRequiredService<IStreamingTransport>();
            var subjects = sp.GetRequiredService<ISubjectResolver>();
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var options = sp.GetRequiredService<IOptions<SubscriberOptions>>();
            var logger = sp.GetRequiredService<ILogger<SubscriptionManager<TRequest, TResponse>>>();

            return new SubscriptionManager<TRequest, TResponse>(
                streamName,
                transport,
                subjects,
                serializer,
                options,
                logger);
        });

        // Register StreamingSubscriber (singleton per TRequest/TResponse)
        services.TryAddSingleton<IStreamingSubscriber<TRequest, TResponse>>(sp =>
        {
            var manager = sp.GetRequiredService<SubscriptionManager<TRequest, TResponse>>();
            var transport = sp.GetRequiredService<IStreamingTransport>();
            var subjects = sp.GetRequiredService<ISubjectResolver>();
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var options = sp.GetRequiredService<IOptions<SubscriberOptions>>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();

            return new StreamingSubscriber<TRequest, TResponse>(
                streamName,
                manager,
                serializer,
                transport,
                subjects,
                options,
                loggerFactory.CreateLogger<StreamingSubscriber<TRequest, TResponse>>());
        });

        return services;
    }
}