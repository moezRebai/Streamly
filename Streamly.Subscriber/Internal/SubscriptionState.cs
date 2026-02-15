using System.Reactive.Subjects;
using Streamly.Core.Models;
using Streamly.Subscriber.Models;

namespace Streamly.Subscriber.Internal;

//Tracking object for this subscription
internal class SubscriptionState<TResponse>
{
    // Internal only — client never sees it. Used for the handshake: subscriber sends it in the request envelope,
    // leader echoes it back in the confirmation so the library can match the 
    // response to the right pending subscription.
    public string CorrelationId { get; init; } = string.Empty;
    public string? RequestId { get; set; }
    public StreamBehavior Behavior { get; init; }
    public Subject<TResponse> Subject { get; } = new();
    public bool WaitingForConfirmation { get; set; } = true;
    public bool IsDisposed { get; set; }
    public long LastKnownEpoch { get; set; }
    public int ReconnectAttempts { get; set; }

    /// <summary>
    /// Called by SubscriptionManager to notify client of status changes.
    /// Set by StreamingSubscriber when creating the state.
    /// </summary>
    internal Action<StreamStatus>? StatusCallback { get; init; }

    /// <summary>
    /// Safely invoke the status callback
    /// </summary>
    internal void NotifyStatus(StreamStatus status)
    {
        try
        {
            StatusCallback?.Invoke(status);
        }
        catch (Exception)
        {
            // Never let callback exceptions bubble into the library
        }
    }
}