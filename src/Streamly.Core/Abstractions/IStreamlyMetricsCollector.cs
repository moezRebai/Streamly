using Streamly.Core.Models;

namespace Streamly.Core.Abstractions;

/// <summary>
/// Records runtime metrics for a single Streamly instance.
/// Implementations must be thread-safe and allocation-free on the hot path.
/// Called from the publisher publish loop and the subscriber dispatch path —
/// any implementation that blocks, allocates, or throws will degrade throughput.
///
/// The null implementation <see cref="NullMetricsCollector"/> is registered
/// by default in both Streamly.Server and Streamly.Client so that monitoring
/// remains strictly opt-in. Call AddStreamlyMonitoring() to activate the
/// in-memory implementation that backs the HTTP endpoints.
/// </summary>
public interface IStreamlyMetricsCollector
{
    // -------------------------------------------------------------------------
    // Publisher — stream lifecycle
    // -------------------------------------------------------------------------

    /// <summary>
    /// Called when a new stream is opened and enters the Streaming state.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    /// <param name="streamName">
    /// The registered stream name, e.g. "FxSpot", "FxSwap", "IRS".
    /// Matches the name used during handler registration.
    /// </param>
    void RecordStreamOpened(string requestId, string streamName);

    /// <summary>
    /// Called when a stream transitions to the Closed state for any reason.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    /// <param name="reason">Why the stream was closed.</param>
    void RecordStreamClosed(string requestId, CloseReason reason);

    // -------------------------------------------------------------------------
    // Publisher — publish decisions
    // -------------------------------------------------------------------------

    /// <summary>
    /// Called after a response passes change detection and is published to NATS.
    /// Must complete in nanoseconds — called on the hot publish path.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    /// <param name="streamName">The registered stream name.</param>
    /// <param name="payloadBytes">Serialized payload size in bytes.</param>
    void RecordPublish(string requestId, string streamName, int payloadBytes);

    /// <summary>
    /// Called when the change detector suppresses a response because it is not
    /// significantly different from the last published image.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    void RecordPublishSkipped(string requestId);

    /// <summary>
    /// Called when a publish attempt fails due to a NATS or serialization error.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    /// <param name="errorMessage">A short description of the failure.</param>
    void RecordPublishError(string requestId, string errorMessage);

    /// <summary>
    /// Called after each successful publish to update the stored latest image.
    ///
    /// Accepts raw JSON as a <see cref="string"/> rather than
    /// <see cref="System.Text.Json.JsonElement"/> to avoid JsonDocument
    /// buffer lifetime issues. A JsonElement is a view into a pooled buffer
    /// owned by its parent JsonDocument — if the caller disposes the document
    /// after passing the element, the stored reference becomes invalid:
    ///
    ///   using var doc = JsonDocument.Parse(...);
    ///   collector.RecordLatestImage(id, name, doc.RootElement); // passes a view
    ///   // doc disposes here → stored JsonElement is now a dangling reference
    ///
    /// Passing a self-contained string eliminates this class of bug entirely.
    /// The caller (ResponsePublisher) already holds the serialized bytes from
    /// the NATS publish step — converting to string is one UTF-8 decode,
    /// cheaper than JsonDocument.Parse().RootElement.Clone().
    ///
    /// The InMemoryMetricsCollector stores the string as-is and writes it
    /// directly into the /streamly/streams HTTP response without re-serializing.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    /// <param name="streamName">The registered stream name.</param>
    /// <param name="rawJson">
    /// The full response payload as a UTF-8 JSON string.
    /// Must be valid JSON — the monitoring layer does not validate it.
    /// </param>
    void RecordLatestImage(string requestId, string streamName, string rawJson);

    // -------------------------------------------------------------------------
    // Subscriber — subscription lifecycle
    // -------------------------------------------------------------------------

    /// <summary>
    /// Called when a new subscription is opened on this instance.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    /// <param name="streamName">The registered stream name.</param>
    void RecordSubscriptionOpened(string requestId, string streamName);

    /// <summary>
    /// Called when a subscription is closed on this instance.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    /// <param name="reason">Why the subscription was closed.</param>
    void RecordSubscriptionClosed(string requestId, CloseReason reason);

    /// <summary>
    /// Called each time a message is received and dispatched to a subscriber.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    void RecordMessageReceived(string requestId);

    /// <summary>
    /// Called exactly once per subscription, when the first data response is received.
    /// Measures the elapsed time from when the subscriber sent the request to when the
    /// first payload (full snapshot) arrived — the end-to-end time-to-first-response.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    /// <param name="elapsedMs">Milliseconds from request sent to first response received.</param>
    void RecordFirstResponseLatency(string requestId, double elapsedMs);

    /// <summary>
    /// Records the end-to-end latency from publisher stamp to subscriber receipt.
    /// Called on the subscriber hot path — must be allocation-free.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    /// <param name="latencyMs">Elapsed milliseconds from publish to receipt.</param>
    void RecordMessageLatency(string requestId, double latencyMs);

    /// <summary>
    /// Called when the subscriber watchdog detects that the publisher has gone
    /// silent beyond the configured keepalive threshold.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    void RecordWatchdogTrigger(string requestId);

    /// <summary>
    /// Called each time the subscriber initiates a reconnection attempt after
    /// detecting a publisher outage.
    /// </summary>
    /// <param name="requestId">The unique request identifier.</param>
    void RecordReconnectionAttempt(string requestId);

    // -------------------------------------------------------------------------
    // Infrastructure — leader election and transport
    // -------------------------------------------------------------------------

    /// <summary>
    /// Called when this instance acquires or loses leadership, or when a new
    /// epoch is observed from a competing candidate.
    /// </summary>
    /// <param name="isLeader">True if this instance is now the leader.</param>
    /// <param name="epoch">The current leader election epoch.</param>
    void RecordLeaderStateChange(bool isLeader, int epoch);

    /// <summary>
    /// Called when the NATS connection state changes — connected, reconnecting,
    /// or disconnected.
    /// </summary>
    /// <param name="isConnected">True if NATS is currently connected.</param>
    void RecordNatsConnectionStateChange(bool isConnected);

    /// <summary>
    /// Called when the subscriber count changes on a stream.
    /// </summary>
    void RecordSubscriberCountChanged(string requestId, int subscriberCount);

    /// <summary>
    /// Stores the serialized request payload for display in the monitoring dashboard.
    /// Called once when a new stream is opened.
    /// </summary>
    void RecordRequestJson(string requestId, string requestJson);
}

/// <summary>
/// No-op implementation of <see cref="IStreamlyMetricsCollector"/>.
/// Registered as the default in Streamly.Server and Streamly.Client so that
/// both packages compile and run correctly when AddStreamlyMonitoring() has
/// not been called. Every method is a true no-op — no allocation, no I/O,
/// no branching beyond the virtual dispatch.
/// </summary>
public sealed class NullMetricsCollector : IStreamlyMetricsCollector
{
    /// <summary>Shared singleton — no state, safe to reuse.</summary>
    public static readonly NullMetricsCollector Instance = new();

    public void RecordStreamOpened(string requestId, string streamName) { }
    public void RecordStreamClosed(string requestId, CloseReason reason) { }
    public void RecordPublish(string requestId, string streamName, int payloadBytes) { }
    public void RecordPublishSkipped(string requestId) { }
    public void RecordPublishError(string requestId, string errorMessage) { }
    public void RecordLatestImage(string requestId, string streamName, string rawJson) { }
    public void RecordSubscriptionOpened(string requestId, string streamName) { }
    public void RecordSubscriptionClosed(string requestId, CloseReason reason) { }
    public void RecordMessageReceived(string requestId) { }
    public void RecordWatchdogTrigger(string requestId) { }
    public void RecordReconnectionAttempt(string requestId) { }
    public void RecordLeaderStateChange(bool isLeader, int epoch) { }
    public void RecordNatsConnectionStateChange(bool isConnected) { }
    
    public void RecordSubscriberCountChanged(string requestId, int subscriberCount) { }
    public void RecordRequestJson(string requestId, string requestJson) { }
    public void RecordMessageLatency(string requestId, double latencyMs) { }
    public void RecordFirstResponseLatency(string requestId, double elapsedMs) { }
}