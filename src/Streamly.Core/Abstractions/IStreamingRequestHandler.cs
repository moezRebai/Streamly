using Streamly.Core.Models;

namespace Streamly.Core.Abstractions;

/// <summary>
/// Implemented by services to handle streaming requests (e.g., FX pricing)
/// Event-driven approach: you push responses when data changes, not polled by framework
/// </summary>
/// <typeparam name="TRequest">Request type (e.g., FxSwapRequest)</typeparam>
/// <typeparam name="TResponse">Response type (e.g., FxSwapPrice)</typeparam>
public interface IStreamingRequestHandler<in TRequest, TResponse>
{
    /// <summary>
    /// Called when a new request is opened or resumed after failover
    /// Subscribe to market data here and return initial response
    /// </summary>
    /// <param name="request">The request to handle</param>
    /// <param name="context">Context for publishing updates</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task OnRequestOpenedAsync(
        TRequest request,
        IStreamingContext<TResponse> context,
        CancellationToken cancellationToken);
    
    /// <summary>
    /// Called when request is closing (cleanup opportunity)
    /// Unsubscribe from market data here
    /// </summary>
    /// <param name="request">The request being closed</param>
    /// <param name="reason">Why it's closing</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task OnRequestClosingAsync(
        TRequest request, 
        CloseReason reason, 
        CancellationToken cancellationToken);
}