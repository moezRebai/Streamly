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

public static class ClientComponentExtensions
{
    /// <summary>
    /// Add subscriber components WITHOUT infrastructure registration.
    /// Use this when infrastructure is already registered (e.g., by AddStreamly).
    /// Optionally supply a custom <see cref="IResponseDiffComputer{TResponse}"/>; defaults to
    /// <see cref="DefaultResponseDiffComputer{TResponse}"/>.
    /// </summary>
    public static IServiceCollection AddClientComponents<TRequest, TResponse>(
        this IServiceCollection services,
        string streamName)
        where TResponse : class
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));

        // Register default diff computer — respects any prior override via TryAddSingleton
        services.TryAddSingleton<IResponseDiffComputer<TResponse>,
            DefaultResponseDiffComputer<TResponse>>();

        // Register SubscriptionManager (singleton per TRequest/TResponse)
        services.TryAddSingleton<SubscriptionManager<TRequest, TResponse>>(sp =>
        {
            var transport    = sp.GetRequiredService<IStreamingTransport>();
            var subjects     = sp.GetRequiredService<ISubjectResolver>();
            var serializer   = sp.GetRequiredService<IMessageSerializer>();
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
                logger);
        });

        // Register StreamingSubscriber (singleton per TRequest/TResponse)
        services.TryAddSingleton<IStreamingSubscriber<TRequest, TResponse>>(sp =>
        {
            var manager = sp.GetRequiredService<SubscriptionManager<TRequest, TResponse>>();
            var transport = sp.GetRequiredService<IStreamingTransport>();
            var subjects = sp.GetRequiredService<ISubjectResolver>();
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var options = sp.GetRequiredService<IOptions<StreamlySettings>>();
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
