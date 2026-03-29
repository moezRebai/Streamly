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
}
