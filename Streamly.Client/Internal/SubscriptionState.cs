using System.Reactive.Subjects;
using Streamly.Core.Models;
using Streamly.Client.Models;

namespace Streamly.Client.Internal;

// Tracking object for this subscription
internal class SubscriptionState<TResponse>
{
    // Internal only — client never sees it. Used for the handshake: subscriber sends it in the request envelope,
    // leader echoes it back in the confirmation so the library can match the
    // response to the right pending subscription.
    public string CorrelationId { get; init; } = string.Empty;
    public string? RequestId { get; set; }
    public StreamBehavior Behavior { get; init; }
    public Subject<TResponse> Subject { get; } = new();
    public bool IsDisposed { get; set; }
    public long LastKnownEpoch { get; set; }
    public int ReconnectAttempts { get; set; }

    /// <summary>
    /// Latest fully merged image maintained by the subscriber.
    /// Null until the first message (full snapshot) is received.
    /// Delta messages are merged onto this before delivery to the handler.
    /// </summary>
    public TResponse? LocalImage { get; set; }

    // RunContinuationsAsynchronously ensures continuations run on the thread pool,
    // not inline on the NATS callback thread that calls TrySetResult.
    public TaskCompletionSource ConfirmationTcs { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Kept as a convenience property so call sites that check WaitingForConfirmation
    // (UnregisterAsync) don't need to change.
    public bool WaitingForConfirmation => !ConfirmationTcs.Task.IsCompleted;

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