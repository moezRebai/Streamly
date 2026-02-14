using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Streamly.Core.Runtime.Configuration;

namespace Streamly.Core.Runtime.Registration;

public class StreamRegistry : IStreamRegistry
{
    private readonly ConcurrentDictionary<Type, string> _streamNames;
    private readonly ILogger<StreamRegistry> _logger;

    public StreamRegistry(StreamlyOptions options, ILogger<StreamRegistry> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _streamNames = new ConcurrentDictionary<Type, string>();
        
        // Load all mappings from options
        foreach (var handler in options.Handlers)
        {
            if (_streamNames.TryAdd(handler.RequestType, handler.StreamName))
            {
                _logger.LogInformation(
                    "Registered stream '{StreamName}' for request type {RequestType}",
                    handler.StreamName,
                    handler.RequestType.Name);
            }
            else
            {
                _logger.LogError(
                    "Duplicate request type {RequestType} - already registered",
                    handler.RequestType.Name);
                throw new InvalidOperationException(
                    $"Request type {handler.RequestType.Name} registered multiple times");
            }
        }
    }

    public string GetStreamName<TRequest>()
    {
        var requestType = typeof(TRequest);
        
        if (_streamNames.TryGetValue(requestType, out var streamName))
        {
            return streamName;
        }

        _logger.LogError(
            "Request type {RequestType} is not registered",
            requestType.Name);
            
        throw new InvalidOperationException(
            $"Request type {requestType.Name} is not registered. " +
            $"Call options.AddHandler<{requestType.Name}, TResponse, THandler>(\"streamName\") in AddStreamly configuration.");
    }

    public bool TryGetStreamName<TRequest>(out string? streamName)
    {
        return _streamNames.TryGetValue(typeof(TRequest), out streamName);
    }

    public bool IsRegistered<TRequest>()
    {
        return _streamNames.ContainsKey(typeof(TRequest));
    }
}