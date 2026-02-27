namespace Streamly.Infrastructure.Nats;

/// <summary>
/// NATS subject naming conventions for Streamly.
/// NATS uses dots (.) as hierarchy separators and supports:
///   *  - single token wildcard  (e.g. "pricing.responses.*")
///   >  - multi-token wildcard   (e.g. "stream.keepalive.>")
///
/// Contrast with Redis channels which used colons (stream:heartbeat).
/// </summary>
public static class NatsSubjectNames
{
    // ── Fixed subjects ───────────────────────────────────────────────────────

    /// <summary>Client → All service instances (broadcast request).</summary>
    public const string PricingRequests = "pricing.requests";

    /// <summary>Leader → All followers (200ms heartbeat).</summary>
    public const string StreamHeartbeat = "stream.heartbeat";

    /// <summary>Request lifecycle events (opened, closed, subscriber changes).</summary>
    public const string StreamEvents = "stream.events";

    /// <summary>Full state sync payload broadcast every 15 seconds.</summary>
    public const string StreamBatch = "stream.batch";

    // ── Dynamic subjects ──────────────────────────────────────────────────────

    /// <summary>
    /// Leader → specific client.
    /// NATS server-side filters so each client only receives its own responses.
    /// Pattern: pricing.responses.{requestId}
    /// </summary>
    public static string PricingResponse(string requestId)
        => $"pricing.responses.{requestId}";

    /// <summary>
    /// Wildcard to subscribe to ALL response subjects (used by subscriber manager).
    /// Pattern: pricing.responses.*
    /// </summary>
    public const string PricingResponsesWildcard = "pricing.responses.*";

    /// <summary>
    /// Publisher keepalive heartbeat.
    /// Pattern: stream.keepalive.{streamName}  (e.g. stream.keepalive.FxSwapPricer)
    /// </summary>
    public static string StreamKeepalive(string streamName)
        => $"stream.keepalive.{streamName}";

    /// <summary>Wildcard to subscribe to ALL keepalive subjects.</summary>
    public const string StreamKeepaliveWildcard = "stream.keepalive.>";

    // ── JetStream KV bucket / key names ──────────────────────────────────────

    /// <summary>Name of the JetStream KV bucket used for leader election.</summary>
    public const string LeaderElectionBucket = "streamly-leader";

    /// <summary>Key within the leader election bucket that holds the current leader ID.</summary>
    public const string LeaderKey = "leader";

    /// <summary>Name of the JetStream KV bucket used for epoch tracking.</summary>
    public const string EpochBucket = "streamly-epoch";

    /// <summary>Key within the epoch bucket.</summary>
    public const string EpochKey = "epoch";
}
