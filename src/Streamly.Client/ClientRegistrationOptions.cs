using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.ChangeDetection;
using Streamly.Core.Configurations;
using Streamly.Infrastructure.Interfaces;
using Streamly.Client.Internal;

namespace Streamly.Client;

public class ClientRegistrationOptions
{
    private readonly IServiceCollection _services;

    internal ClientRegistrationOptions(IServiceCollection services)
    {
        _services = services;
    }

    /// <summary>
    /// Register a subscriber for a stream type.
    /// streamName must match what the publisher registered (e.g., "SpotPricer")
    /// </summary>
    public void AddSubscriber<TRequest, TResponse>(string streamName)
        where TResponse : class
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));

        // Register default diff computer — respects any prior override via TryAddSingleton
        _services.TryAddSingleton<IResponseDiffComputer<TResponse>,
            DefaultResponseDiffComputer<TResponse>>();

        _services.TryAddSingleton<SubscriptionManager<TRequest, TResponse>>(sp =>
        {
            var transport    = sp.GetRequiredService<IStreamingTransport>();
            var subjects     = sp.GetRequiredService<ISubjectResolver>();
            var serializer   = sp.GetRequiredService<IMessageSerializer>();
            var metrics   = sp.GetRequiredService<IStreamlyMetricsCollector>();
            var diffComputer = sp.GetRequiredService<IResponseDiffComputer<TResponse>>();
            var options      = sp.GetRequiredService<IOptions<StreamlySettings>>();
            var logger       = sp.GetRequiredService<ILogger<SubscriptionManager<TRequest, TResponse>>>();

            return new SubscriptionManager<TRequest, TResponse>(
                streamName,
                transport,
                subjects,
                serializer,
                diffComputer,
                options,
                metrics,
                logger);
        });

        // Register StreamingSubscriber behind public interface (singleton per TRequest/TResponse)
        _services.TryAddSingleton<IStreamingSubscriber<TRequest, TResponse>>(sp =>
        {
            var manager       = sp.GetRequiredService<SubscriptionManager<TRequest, TResponse>>();
            var transport     = sp.GetRequiredService<IStreamingTransport>();
            var subjects      = sp.GetRequiredService<ISubjectResolver>();
            var serializer    = sp.GetRequiredService<IMessageSerializer>();
            var metrics       = sp.GetRequiredService<IStreamlyMetricsCollector>();
            var options       = sp.GetRequiredService<IOptions<StreamlySettings>>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();

            return new StreamingSubscriber<TRequest, TResponse>(
                streamName,
                manager,
                serializer,
                metrics,
                transport,
                subjects,
                options,
                loggerFactory.CreateLogger<StreamingSubscriber<TRequest, TResponse>>());
        });
    }
}
