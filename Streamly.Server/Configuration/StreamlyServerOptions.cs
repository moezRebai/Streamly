using Streamly.Core.Abstractions;

namespace Streamly.Server.Configuration;

/// <summary>
/// Options for configuring Streamly server handlers only.
/// Infrastructure settings (NATS, LeaderElection) come from appsettings.json.
/// </summary>
public class StreamlyServerOptions
{
    internal List<HandlerRegistration> Handlers { get; } = new();

    /// <summary>
    /// Register a streaming handler using the default diff computer.
    /// </summary>
    public void AddHandler<TRequest, TResponse, THandler>(string streamName)
        where THandler : class, IStreamingRequestHandler<TRequest, TResponse>
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));

        Handlers.Add(new HandlerRegistration(
            typeof(TRequest),
            typeof(TResponse),
            typeof(THandler),
            streamName));
    }

    /// <summary>
    /// Register a streaming handler with a custom diff computer.
    /// Use this when the default reflection-based computer is insufficient
    /// (e.g. deep collection diffing, custom equality semantics).
    /// </summary>
    public void AddHandler<TRequest, TResponse, THandler, TDiffComputer>(string streamName)
        where TResponse     : class
        where THandler      : class, IStreamingRequestHandler<TRequest, TResponse>
        where TDiffComputer : class, IResponseDiffComputer<TResponse>
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));

        Handlers.Add(new HandlerRegistration(
            typeof(TRequest),
            typeof(TResponse),
            typeof(THandler),
            streamName,
            typeof(TDiffComputer)));
    }
}
