namespace Streamly.Core.Runtime.RequestManagement;

/// <summary>
/// Thread-safe registry for active streaming requests
/// </summary>
internal interface IRequestRegistry<TRequest, TResponse>
{
    /// <summary>
    /// Try to add a new request to the registry
    /// </summary>
    /// <returns>True if added, false if already exists</returns>
    bool TryAdd(string requestId, RequestMetadata<TRequest, TResponse> metadata);
    
    /// <summary>
    /// Try to get request metadata
    /// </summary>
    bool TryGet(string requestId, out RequestMetadata<TRequest, TResponse>? metadata);
    
    /// <summary>
    /// Try to update request metadata atomically
    /// </summary>
    /// <param name="updater">Action to modify metadata (called inside lock)</param>
    /// <returns>True if found and updated, false if not found</returns>
    bool TryUpdate(string requestId, Action<RequestMetadata<TRequest, TResponse>> updater);
    
    /// <summary>
    /// Try to remove a request from the registry
    /// </summary>
    bool TryRemove(string requestId, out RequestMetadata<TRequest, TResponse>? metadata);
    
    /// <summary>
    /// Get all request IDs
    /// </summary>
    IEnumerable<string> GetAllRequestIds();
    
    /// <summary>
    /// Get all request metadata (snapshot)
    /// </summary>
    IEnumerable<RequestMetadata<TRequest, TResponse>> GetAll();
    
    /// <summary>
    /// Get count of active requests
    /// </summary>
    int Count { get; }
    
    /// <summary>
    /// Check if request exists
    /// </summary>
    bool Contains(string requestId);
    
    /// <summary>
    /// Clear all requests (used during shutdown)
    /// </summary>
    void Clear();
}