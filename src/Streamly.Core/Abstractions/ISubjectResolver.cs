namespace Streamly.Core.Abstractions;

/// <summary>
/// Resolves transport-specific subject/channel names.
/// Redis uses colons (stream:heartbeat), NATS uses dots (stream.heartbeat).
/// 
/// Design principle: Domain-agnostic - works for ANY request/response types.
/// Stream names come from [StreamName] attribute on request classes.
/// </summary>
public interface ISubjectResolver
{
    /// <summary>
    /// Subject for clients to publish requests to a specific stream.
    /// All service instances subscribe to this.
    /// </summary>
    /// <param name="streamName">Stream identifier (e.g., "FxSwapPricer", "IrsCalculator")</param>
    string GetRequestsSubject(string streamName);

    /// <summary>
    /// Subject for service to publish responses.
    /// NATS: Per-request subject for server-side filtering
    /// </summary>
    /// <param name="streamName">Stream identifier</param>
    /// <param name="requestId">Request identifier (used in NATS for per-client subjects)</param>
    string GetResponsesSubject(string streamName, string? requestId = null);

    /// <summary>
    /// Wildcard pattern to subscribe to ALL responses for a stream.
    /// Redis: "streams.responses.{streamName}" (client-side filtering)
    /// NATS: "streams.responses.{streamName}.*" (server-side filtering)
    /// </summary>
    /// <param name="streamName">Stream identifier</param>
    string GetResponsesWildcard(string streamName);

    /// <summary>Leader → All followers heartbeat (200ms interval).</summary>
    /// <param name="streamName">Stream identifier</param>
    string GetHeartbeatSubject(string streamName);

    /// <summary>
    /// Request close event notifications from leader to all instances.
    /// Used when leader closes a request (error, timeout, unsubscribe).
    /// All instances subscribe to this to synchronize request closure.
    /// </summary>
    /// <param name="streamName">Stream identifier</param>
    string GetCloseEventsSubject(string streamName);

    /// <summary>Full state sync payload broadcast (every 15s).</summary>
    /// <param name="streamName">Stream identifier</param>
    string GetBatchSyncSubject(string streamName);

    /// <summary>Publisher keepalive heartbeat.</summary>
    /// <param name="streamName">Stream identifier</param>
    string GetKeepaliveSubject(string streamName);

    /// <summary>Wildcard for all keepalive subjects (for monitoring).</summary>
    string GetKeepaliveWildcard();

    /// <summary>
    /// Unsubscribe notifications from subscribers.
    /// Subscribers publish to this when they unsubscribe.
    /// Leader listens to manage subscriber counts and auto-close Live streams.
    /// </summary>
    /// <param name="streamName">Stream identifier</param>
    string GetUnsubscribeSubject(string streamName);

    /// <summary>
    /// Confirmation messages from leader to subscribers.
    /// Leader publishes RequestId confirmation after computing SHA256 hash.
    /// Subscribers wait for this to know their actual RequestId.
    /// </summary>
    /// <param name="streamName">Stream identifier</param>
    string GetConfirmSubject(string streamName);

    public string GetInstancesBucketSubject();

    public string GetLeaderElectionBucketSubject();
    
    // Global — one subject for all subscriber heartbeats, not per-stream
    // A subscriber is either alive or dead, not per-stream
    public string GetSubscriberHeartbeatSubject();
}