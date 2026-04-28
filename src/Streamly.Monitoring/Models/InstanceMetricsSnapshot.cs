namespace Streamly.Monitoring.Models;

/// <summary>
/// A point-in-time snapshot of all counters for a single Streamly instance.
/// Returned by GET /streamly/metrics as JSON.
/// Also consumed by Streamly.Dashboard when it polls each instance.
///
/// All counters are cumulative since the process started unless noted otherwise.
/// </summary>
public sealed record InstanceMetricsSnapshot
{
    // ── Identity ──────────────────────────────────────────────────────────────

    /// <summary>Unique identifier for this instance (from MonitoringOptions).</summary>
    public required string InstanceId { get; init; }

    /// <summary>Role label: "Publisher", "Subscriber", or "Both".</summary>
    public required string InstanceRole { get; init; }

    /// <summary>UTC time this process started.</summary>
    public required DateTimeOffset StartedAt { get; init; }

    /// <summary>UTC time this snapshot was produced.</summary>
    public required DateTimeOffset SnapshotAt { get; init; }

    // ── Leadership ────────────────────────────────────────────────────────────

    /// <summary>True if this instance is currently the stream leader.</summary>
    public required bool IsLeader { get; init; }

    /// <summary>Current leader election epoch.</summary>
    public required int LeaderEpoch { get; init; }

    /// <summary>True if the NATS connection is currently established.</summary>
    public required bool NatsConnected { get; init; }

    // ── Publisher — stream counters ───────────────────────────────────────────

    /// <summary>Number of streams currently in the Streaming state.</summary>
    public required int ActiveStreams { get; init; }

    /// <summary>Total streams opened since process start.</summary>
    public required long TotalStreamsOpened { get; init; }

    /// <summary>Total streams closed since process start (all reasons).</summary>
    public required long TotalStreamsClosed { get; init; }

    // ── Publisher — publish counters ──────────────────────────────────────────

    /// <summary>Total prices published to NATS since process start.</summary>
    public required long TotalPublishes { get; init; }

    /// <summary>
    /// Total publishes suppressed by the change detector since process start.
    /// A high skip ratio (skips / (publishes + skips)) indicates the market
    /// is stable or the change threshold is set too wide.
    /// </summary>
    public required long TotalPublishSkips { get; init; }

    /// <summary>Total publish errors since process start.</summary>
    public required long TotalPublishErrors { get; init; }

    /// <summary>
    /// Rolling publish rate in messages per second, computed over the
    /// last PublishRateWindow (default 60 seconds).
    /// </summary>
    public required double PublishRatePerSec { get; init; }

    // ── Subscriber — counters ─────────────────────────────────────────────────

    /// <summary>Number of subscriptions currently active on this instance.</summary>
    public required int ActiveSubscriptions { get; init; }

    /// <summary>
    /// Number of subscriptions in the Pending state — attempted but not yet confirmed by any publisher.
    /// Included in ActiveStreams. A non-zero value on a subscriber instance indicates streams for which
    /// no cluster has accepted the request.
    /// </summary>
    public int PendingSubscriptions { get; init; }

    /// <summary>
    /// Active subscriber-side streams: confirmed subscriptions + pending (unconfirmed) attempts.
    /// Zero on a pure publisher instance.
    /// </summary>
    public int SubscriberActiveStreams { get; init; }

    /// <summary>
    /// Active publisher-side streams: unique requests currently being served.
    /// Zero on a pure subscriber instance.
    /// </summary>
    public int PublisherActiveStreams { get; init; }

    /// <summary>Total subscriptions opened since process start.</summary>
    public required long TotalSubscriptionsOpened { get; init; }

    /// <summary>Total subscriptions closed since process start.</summary>
    public required long TotalSubscriptionsClosed { get; init; }

    /// <summary>Total messages received and dispatched since process start.</summary>
    public required long TotalMessagesReceived { get; init; }

    /// <summary>
    /// Total watchdog triggers since process start.
    /// Each trigger represents a publisher heartbeat timeout on one stream.
    /// </summary>
    public required long TotalWatchdogTriggers { get; init; }

    /// <summary>Total reconnection attempts since process start.</summary>
    public required long TotalReconnectionAttempts { get; init; }

    /// <summary>
    /// Total confirmation send failures since process start (all retries exhausted).
    /// Each increment means a subscriber will fall back to its own reconnection timeout.
    /// A non-zero value warrants investigating NATS transport stability.
    /// </summary>
    public required long TotalConfirmationFailures { get; init; }

    // ── Process — CPU and memory ──────────────────────────────────────────────

    /// <summary>
    /// CPU usage % across all logical cores, averaged over the last 5-second sample window.
    /// 0 until the first background sample completes (~5 s after startup).
    /// </summary>
    public required double CpuPercent { get; init; }

    /// <summary>OS working set in bytes — total physical RAM used by this process.</summary>
    public required long WorkingSetBytes { get; init; }

    /// <summary>Managed GC heap in bytes — excludes native/unmanaged allocations.</summary>
    public required long GcHeapBytes { get; init; }
}
