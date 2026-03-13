using Streamly.Core.Abstractions;

namespace Streamly.Infrastructure.Nats;

/// <summary>
/// NATS subject naming convention: dots as separators with server-side wildcard support.
/// Example: "streams.requests.FxSwapPricer", "streams.responses.FxSwapPricer.req-abc123"
/// 
/// NATS uses per-request response subjects for server-side filtering (more efficient).
/// Wildcards: "*" (single token), ">" (multi-token)
/// </summary>
public sealed class NatsSubjectResolver : ISubjectResolver
{
    public string GetRequestsSubject(string streamName)
        => $"streams.requests.{streamName}";

    public string GetResponsesSubject(string streamName, string? requestId = null)
        => requestId != null 
            ? $"streams.responses.{streamName}.{requestId}"  // Per-request subject
            : $"streams.responses.{streamName}";             // Base subject

    public string GetResponsesWildcard(string streamName)
        => $"streams.responses.{streamName}.*";  // NATS server-side wildcard

    public string GetHeartbeatSubject(string streamName)
        => $"streams.heartbeat.{streamName}";

    public string GetCloseEventsSubject(string streamName)
        => $"streams.close-events.{streamName}";

    public string GetBatchSyncSubject(string streamName)
        => $"streams.batch.{streamName}";

    public string GetKeepaliveSubject(string streamName)
        => $"streams.keepalive.{streamName}";

    public string GetKeepaliveWildcard()
        => "streams.keepalive.>";  // NATS multi-level wildcard

    public string GetUnsubscribeSubject(string streamName)
        => $"streams.unsubscribe.{streamName}";

    public string GetConfirmSubject(string streamName)
        => $"streams.confirm.{streamName}";


    public string GetInstancesBucketSubject()
        => "streamly-instances";

    public string GetLeaderElectionBucketSubject()
        => "streamly-leader";
    
    // Global — one subject for all subscriber heartbeats, not per-stream
// A subscriber is either alive or dead, not per-stream
    public string GetSubscriberHeartbeatSubject()
        => "streams.subscriber-heartbeat";
}