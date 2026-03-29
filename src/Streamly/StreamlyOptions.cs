using Streamly.Core.Abstractions;
using Streamly.Client;
using Streamly.Server.Configuration;

namespace Streamly;

/// <summary>
/// Unified options for configuring both Streamly server handlers and client subscribers.
/// Infrastructure settings (NATS, LeaderElection) come from appsettings.json.
/// </summary>
public class StreamlyOptions
{
    // Deferred configuration actions for server-side handlers
    private readonly List<Action<StreamlyServerOptions>> _serverConfigurators = new();

    // Deferred configuration actions for client-side subscribers
    private readonly List<Action<ClientRegistrationOptions>> _clientConfigurators = new();

    internal bool HasHandlers => _serverConfigurators.Count > 0;
    internal bool HasSubscribers => _clientConfigurators.Count > 0;

    internal void ApplyServerOptions(StreamlyServerOptions serverOptions)
    {
        foreach (var configurator in _serverConfigurators)
            configurator(serverOptions);
    }

    internal void ApplyClientOptions(ClientRegistrationOptions clientOptions)
    {
        foreach (var configurator in _clientConfigurators)
            configurator(clientOptions);
    }

    /// <summary>
    /// Register a server-side streaming handler.
    /// </summary>
    public void AddHandler<TRequest, TResponse, THandler>(string streamName)
        where THandler : class, IStreamingRequestHandler<TRequest, TResponse>
    {
        _serverConfigurators.Add(opts =>
            opts.AddHandler<TRequest, TResponse, THandler>(streamName));
    }

    /// <summary>
    /// Register a client-side subscriber for a stream type.
    /// streamName must match what the publisher registered.
    /// </summary>
    public void AddSubscriber<TRequest, TResponse>(string streamName)
        where TResponse : class
    {
        _clientConfigurators.Add(opts =>
            opts.AddSubscriber<TRequest, TResponse>(streamName));
    }
}
