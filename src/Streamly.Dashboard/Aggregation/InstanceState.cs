using Streamly.Monitoring.Models;

namespace Streamly.Dashboard.Aggregation;

/// <summary>
/// Runtime state for a single monitored Streamly instance.
/// The MetricsAggregator updates this after every poll cycle;
/// Blazor components read it (read-only) via InstanceRegistry.
/// </summary>
public sealed class InstanceState
{
    /// <summary>
    /// Configuration entry this state corresponds to.
    /// </summary>
    public required InstanceConfig Config { get; init; }

    /// <summary>
    /// Most recent metrics snapshot from GET /streamly/metrics.
    /// Null until the first successful poll or when the instance is unreachable.
    /// </summary>
    public InstanceMetricsSnapshot? Metrics { get; set; }

    /// <summary>
    /// Most recent stream list from GET /streamly/streams.
    /// Empty array until the first successful poll.
    /// </summary>
    public StreamDetailRecord[] Streams { get; set; } = [];

    /// <summary>
    /// False if the last poll attempt timed out or threw an exception.
    /// The aggregator continues retrying on every subsequent cycle.
    /// </summary>
    public bool IsReachable { get; set; }

    /// <summary>
    /// Wall-clock time of the last completed poll attempt (regardless of success).
    /// </summary>
    public DateTimeOffset LastPolledAt { get; set; }

    /// <summary>
    /// Error message from the last failed poll, or null on success.
    /// </summary>
    public string? LastErrorMessage { get; set; }

    // ── Derived convenience properties used by UI components ──────────────

    /// <summary>
    /// Three-state health indicator for the status dot in the UI.
    /// </summary>
    public InstanceHealth Health =>
        !IsReachable           ? InstanceHealth.Unreachable :
        Metrics is null        ? InstanceHealth.Unreachable :
        !Metrics.NatsConnected ? InstanceHealth.Degraded    :
                                 InstanceHealth.Healthy;

    /// <summary>
    /// Role string from the most recent snapshot, or "Unknown" if unavailable.
    /// </summary>
    public string Role => Metrics?.InstanceRole ?? "Unknown";

    /// <summary>
    /// Whether this instance is currently the NATS cluster leader.
    /// </summary>
    public bool IsLeader => Metrics?.IsLeader ?? false;
}

/// <summary>
/// Traffic-light health state for a monitored instance.
/// </summary>
public enum InstanceHealth
{
    /// <summary>Connected and NATS is up.</summary>
    Healthy,

    /// <summary>Connected but NATS disconnected or metrics indicate a problem.</summary>
    Degraded,

    /// <summary>Did not respond within the poll timeout.</summary>
    Unreachable
}
