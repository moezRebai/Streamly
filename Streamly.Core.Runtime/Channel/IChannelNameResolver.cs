namespace Streamly.Core.Runtime.Channel;

/// <summary>
/// Resolves Redis channel names for a given stream name
///
/// Channel map:
///   streams.requests.{stream}    → Client → Service (subscribe)
///   streams.confirm.{stream}     → Leader → Client (confirmation + RequestId)
///   streams.responses.{stream}   → Leader → Client (price updates)
///   streams.unsubscribe.{stream} → Client → Service (unsubscribe)
///   streams.heartbeat.{stream}   → Leader → Followers (200ms)
///   streams.events.{stream}      → Leader → All instances (close events)
///   streams.batch.{stream}       → Leader → All instances (state sync 15s)
///   streamly:leader:{stream}     → Redis key (leader lock)
/// </summary>
public interface IChannelNameResolver
{
    /// <summary>Client → Service: subscribe request</summary>
    string GetRequestsChannel(string streamName);

    /// <summary>Leader → Client: confirmation with real RequestId</summary>
    string GetConfirmChannel(string streamName);

    /// <summary>Leader → Client: response updates</summary>
    string GetResponsesChannel(string streamName);

    /// <summary>Client → Service: unsubscribe signal</summary>
    string GetUnsubscribeChannel(string streamName);

    /// <summary>Leader → Followers: 200ms heartbeat</summary>
    string GetHeartbeatChannel(string streamName);

    /// <summary>Leader → All instances: close events</summary>
    string GetEventsChannel(string streamName);

    /// <summary>Leader → All instances: full state sync every 15s</summary>
    string GetBatchChannel(string streamName);

    /// <summary>Redis key for leader lock (not a channel)</summary>
    string GetLeaderLockKey(string streamName);
}