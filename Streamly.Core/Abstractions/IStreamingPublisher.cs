using Streamly.Core.Models;

namespace Streamly.Core.Abstractions;

/// <summary>
/// Service-side API for publishing streaming requests
/// </summary>
/// <typeparam name="TRequest">Request type</typeparam>
/// <typeparam name="TResponse">Response type</typeparam>
public interface IStreamingPublisher<TRequest, TResponse>
{
    /// <summary>
    /// Start handling a streaming request (idempotent - same request = same ID)
    /// </summary>
    Task<string> OpenRequestAsync(
        TRequest request, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Explicitly close a streaming request
    /// </summary>
    Task CloseRequestAsync(
        string requestId, 
        CloseReason reason = CloseReason.Normal,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Get current state of a request (if it exists)
    /// </summary>
    Task<StreamingRequest<TRequest>?> GetRequestAsync(
        string requestId, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Get all active request IDs
    /// </summary>
    Task<string[]> GetActiveRequestsAsync(
        CancellationToken cancellationToken = default);
}