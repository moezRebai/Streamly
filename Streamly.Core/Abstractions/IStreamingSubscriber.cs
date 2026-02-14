using Streamly.Core.Models;

namespace Streamly.Core.Abstractions;

/// <summary>
/// Client-side API for subscribing to streaming responses
/// </summary>
/// <typeparam name="TRequest">Request type</typeparam>
/// <typeparam name="TResponse">Response type</typeparam>
public interface IStreamingSubscriber<in TRequest, TResponse>
{
    /// <summary>
    /// Subscribe to a streaming request
    /// </summary>
    Task<SubscriptionContext> SubscribeAsync(
        TRequest request,
        Action<StreamingResponse<TResponse>> onResponse,
        Action<Exception>? onError = null,
        Action<CloseReason>? onComplete = null,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Unsubscribe from a request
    /// </summary>
    Task UnsubscribeAsync(
        string requestId, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Unsubscribe from all active subscriptions
    /// </summary>
    Task UnsubscribeAllAsync(CancellationToken cancellationToken = default);
}