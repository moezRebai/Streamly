using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Infrastructure.Interfaces;
using Streamly.Subscriber.Configuration;
using Streamly.Subscriber.Internal;

namespace Streamly.Subscriber;

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
    public void AddSubscriber<TRequest, TResponse>(string streamName)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));

        // Register SubscriptionManager (singleton per TRequest/TResponse)
        // streamName captured in closure - no IStreamRegistry needed
        _services.TryAddSingleton<SubscriptionManager<TRequest, TResponse>>(sp =>
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

        // Register StreamingSubscriber behind public interface (singleton per TRequest/TResponse)
        _services.TryAddSingleton<IStreamingSubscriber<TRequest, TResponse>>(sp =>
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
    }
}