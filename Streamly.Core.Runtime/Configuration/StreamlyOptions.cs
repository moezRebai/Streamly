using Streamly.Core.Abstractions;

namespace Streamly.Core.Runtime.Configuration;

/// <summary>
/// Options for configuring Streamly handlers only
/// Infrastructure settings (Redis, LeaderElection) come from appsettings.json
/// </summary>
public class StreamlyOptions
{
    internal List<HandlerRegistration> Handlers { get; } = new();
    
    /// <summary>
    /// Register a streaming handler
    /// </summary>
    public StreamlyOptions AddHandler<TRequest, TResponse, THandler>(string streamName)
        where THandler : class, IStreamingRequestHandler<TRequest, TResponse>
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));
        
        Handlers.Add(new HandlerRegistration(
            typeof(TRequest),
            typeof(TResponse),
            typeof(THandler),
            streamName));
        
        return this;
    }
}