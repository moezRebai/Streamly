using Streamly.Core.Models;

namespace Streamly.Core.Runtime.RequestManagement;

/// <summary>
/// Manages the lifecycle of streaming requests
/// - Opens requests (idempotent via deterministic hashing)
/// - Coordinates with leader election
/// - Publishes batch sync (if leader)
/// - Recovers missing requests from batch sync
/// - Closes requests and invokes handler cleanup
/// </summary>
internal interface IRequestManager<TRequest, TResponse>
{
    /// <summary>
    /// Stream name this manager handles
    /// </summary>
    string StreamName { get; }
    
    /// <summary>
    /// Open a streaming request (idempotent - same request = same ID)
    /// Called when request arrives from Redis
    /// </summary>
    Task<string> OpenRequestAsync(RequestEnvelope<TRequest> envelope, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Close a streaming request
    /// </summary>
    Task CloseRequestAsync(string requestId, CloseReason reason, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Get request metadata (if exists)
    /// </summary>
    RequestMetadata<TRequest, TResponse>? GetRequest(string requestId);
    
    /// <summary>
    /// Get all active request IDs
    /// </summary>
    string[] GetActiveRequestIds();
    
    /// <summary>
    /// Get count of active requests
    /// </summary>
    int ActiveRequestCount { get; }
    
    /// <summary>
    /// Start the request manager
    /// - Subscribe to Redis channels
    /// - Start batch sync loop (if leader)
    /// </summary>
    Task StartAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Stop the request manager
    /// - Close all requests
    /// - Unsubscribe from channels
    /// </summary>
    Task StopAsync(CancellationToken cancellationToken = default);
}