namespace Streamly.Core.Configurations;

/// <summary>
/// Unified Streamly configuration
/// </summary>
public class StreamlySettings
{
    public const string SectionName = "Streamly";

    /// <summary>
    /// Logical name for this service instance (e.g. "SpotPricer", "FxSwapSubscriber").
    /// Used to build InstanceId if not explicitly set.
    /// </summary>
    public string ServiceName { get; set; } = "StreamlyService";

    /// <summary>
    /// Unique identifier for this process instance. Defaults to "{ServiceName}-{MachineName}".
    /// Must be unique across all running instances.
    /// </summary>
    public string InstanceId
    {
        get => _instanceId ?? $"{ServiceName}-{Environment.MachineName}";
        set => _instanceId = value;
    }

    private string? _instanceId;

    /// <summary>
    /// How long without a subscriber heartbeat before the subscriber lease is considered expired.
    /// Default: 10 000 ms. Subscriber sends heartbeats every ~3 s → 3× safety margin.
    /// </summary>
    public int SubscriberHeartbeatTimeoutMs { get; set; } = 10_000;

    /// <summary>
    /// How often the leader publishes its keepalive signal.
    /// Must be well below the subscriber HeartbeatTimeout. Default: 500 ms.
    /// </summary>
    public int PublisherHeartbeatIntervalMs { get; set; } = 500;

    public LeaderElectionSection LeaderElection { get; set; } = new();
    public StateSyncSection StateSync { get; set; } = new();
    public SubscriberSection Subscriber { get; set; } = new();

    public class LeaderElectionSection
    {
        /// <summary>Interval at which the leader publishes heartbeats. Default: 200 ms.</summary>
        public int HeartbeatIntervalMs { get; set; } = 200;

        /// <summary>No heartbeat for this duration → leader considered dead. Default: 600 ms.</summary>
        public int DeadThresholdMs { get; set; } = 600;
    }

    public class StateSyncSection
    {
        /// <summary>Interval for batch state-sync publishing. Default: 15 000 ms.</summary>
        public int BatchSyncIntervalMs { get; set; } = 15_000;
    }

    public class SubscriberSection
    {
        /// <summary>Timeout waiting for publisher confirmation after opening a stream. Default: 10 000 ms.</summary>
        public int ConfirmationTimeoutMs { get; set; } = 10_000;

        /// <summary>Parallel dispatch workers per stream type. Default: processor count.</summary>
        public int DispatchWorkerCount { get; set; } = Environment.ProcessorCount;

        /// <summary>Internal dispatch channel capacity (backpressure buffer). Default: 10 000.</summary>
        public int DispatchChannelCapacity { get; set; } = 10_000;

        /// <summary>Max reconnect attempts before giving up. Default: 20.</summary>
        public int MaxReconnectAttempts { get; set; } = 60;

        /// <summary>Initial delay before the first reconnect attempt. Default: 1 000 ms.</summary>
        public int ReconnectInitialDelayMs { get; set; } = 1_000;

        /// <summary>No response for this duration → publisher considered dead. Default: 1 500 ms.</summary>
        public int HeartbeatTimeoutMs { get; set; } = 1_500;

        /// <summary>How often the subscriber sends heartbeats to the publisher. Default: 3 000 ms.</summary>
        public int HeartbeatIntervalMs { get; set; } = 3_000;

        /// <summary>
        /// Max messages buffered per request while waiting for confirmation.
        /// Oldest entries are evicted when the limit is reached (guards against confirmation loss).
        /// Default: 50.
        /// </summary>
        public int PendingResponseBufferLimit { get; set; } = 50;
    }
}