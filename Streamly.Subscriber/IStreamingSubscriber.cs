using System;
using Streamly.Core.Models;

namespace Streamly.Subscriber;

/// <summary>
/// Client-side API for subscribing to streaming responses
/// Clean interface - client knows nothing about Redis, RequestId, or CorrelationId
/// </summary>
/// <typeparam name="TRequest">Request type (e.g., SpotRequest)</typeparam>
/// <typeparam name="TResponse">Response type (e.g., SpotPrice)</typeparam>
public interface IStreamingSubscriber<TRequest, TResponse>
{
    /// <summary>
    /// Subscribe to a streaming request
    /// Returns IObservable that emits responses as they arrive
    ///
    /// StreamBehavior.Live:
    ///   - Stream stays open until explicitly unsubscribed or service closes
    ///   - Auto-reconnects on error/timeout/shutdown (with Polly backoff)
    ///   - Auto-closes when subscriber count reaches 0
    ///
    /// StreamBehavior.Snapshot:
    ///   - Emits exactly one response then completes
    ///   - No reconnect after completion
    ///   - Framework auto-closes stream after first response
    ///
    /// Usage:
    ///   var stream = subscriber.Stream(request, StreamBehavior.Live);
    ///   var subscription = stream
    ///       .Where(p => p.Rate > 1.08)
    ///       .Subscribe(
    ///           onNext: price => Console.WriteLine(price.Rate),
    ///           onError: ex => Console.WriteLine(ex.Message),
    ///           onCompleted: () => Console.WriteLine("Done"));
    ///
    ///   // Later, to unsubscribe:
    ///   subscription.Dispose();
    /// </summary>
    /// <param name="request">Request describing what data to stream</param>
    /// <param name="behavior">Live (default) or Snapshot</param>
    /// <returns>Observable sequence of responses</returns>
    IObservable<TResponse> Stream(
        TRequest request,
        StreamBehavior behavior = StreamBehavior.Live);
}