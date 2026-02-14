using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Streamly.Core.Runtime.RequestManagement;

/// <summary>
/// Thread-safe implementation of request registry using ConcurrentDictionary
/// </summary>
internal class RequestRegistry<TRequest, TResponse>(ILogger<RequestRegistry<TRequest, TResponse>> logger)
    : IRequestRegistry<TRequest, TResponse>
{
    private readonly ConcurrentDictionary<string, RequestMetadata<TRequest, TResponse>> _requests = new();
    private readonly ILogger<RequestRegistry<TRequest, TResponse>> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    public int Count => _requests.Count;

    public bool TryAdd(string requestId, RequestMetadata<TRequest, TResponse> metadata)
    {
        if (string.IsNullOrWhiteSpace(requestId))
            throw new ArgumentException("Request ID cannot be null or whitespace", nameof(requestId));
        if (metadata == null)
            throw new ArgumentNullException(nameof(metadata));

        var added = _requests.TryAdd(requestId, metadata);
        
        if (added)
        {
            _logger.LogDebug(
                "Added request '{RequestId}' to registry (total: {Count})",
                requestId,
                _requests.Count);
        }
        else
        {
            _logger.LogDebug(
                "Request '{RequestId}' already exists in registry",
                requestId);
        }
        
        return added;
    }

    public bool TryGet(string requestId, out RequestMetadata<TRequest, TResponse>? metadata)
    {
        if (string.IsNullOrWhiteSpace(requestId))
        {
            metadata = null;
            return false;
        }

        return _requests.TryGetValue(requestId, out metadata);
    }

    public bool TryUpdate(string requestId, Action<RequestMetadata<TRequest, TResponse>> updater)
    {
        if (string.IsNullOrWhiteSpace(requestId))
            throw new ArgumentException("Request ID cannot be null or whitespace", nameof(requestId));
        if (updater == null)
            throw new ArgumentNullException(nameof(updater));

        if (_requests.TryGetValue(requestId, out var metadata))
        {
            // Lock on the metadata object to prevent concurrent updates
            lock (metadata)
            {
                updater(metadata);
            }
            
            _logger.LogTrace(
                "Updated request '{RequestId}' in registry",
                requestId);
            
            return true;
        }

        _logger.LogDebug(
            "Cannot update request '{RequestId}' - not found in registry",
            requestId);
        
        return false;
    }

    public bool TryRemove(string requestId, out RequestMetadata<TRequest, TResponse>? metadata)
    {
        if (string.IsNullOrWhiteSpace(requestId))
        {
            metadata = null;
            return false;
        }

        var removed = _requests.TryRemove(requestId, out metadata);
        
        if (removed)
        {
            _logger.LogDebug(
                "Removed request '{RequestId}' from registry (remaining: {Count})",
                requestId,
                _requests.Count);
        }
        else
        {
            _logger.LogDebug(
                "Cannot remove request '{RequestId}' - not found in registry",
                requestId);
        }
        
        return removed;
    }

    public IEnumerable<string> GetAllRequestIds()
    {
        return _requests.Keys.ToList();
    }

    public IEnumerable<RequestMetadata<TRequest, TResponse>> GetAll()
    {
        return _requests.Values.ToList();
    }

    public bool Contains(string requestId)
    {
        if (string.IsNullOrWhiteSpace(requestId))
            return false;

        return _requests.ContainsKey(requestId);
    }

    public void Clear()
    {
        var count = _requests.Count;
        _requests.Clear();
        
        _logger.LogInformation(
            "Cleared registry, removed {Count} requests",
            count);
    }
}