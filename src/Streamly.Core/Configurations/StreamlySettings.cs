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
    /// Identifies the cluster this instance belongs to.
    /// Instances sharing the same ClusterId compete for leadership together.
    /// Instances in different clusters run independent elections and never cross-handle requests.
    /// Defaults to "default" for backward compatibility (single-cluster deployments).
    /// </summary>
    public string ClusterId { get; set; } = "default";

    /// <summary>
    /// How long without a publisher keepalive before a subscriber considers the publisher dead.
    /// Default: 10 000 ms — publisher sends keepalives every <see cref="PublisherHeartbeatIntervalMs"/> (500 ms),
    /// giving a 20× safety margin to absorb transient network delays before triggering reconnection.
    /// </summary>
    public int SubscriberHeartbeatTimeoutMs { get; set; } = 10_000;

    /// <summary>
    /// How often the leader broadcasts a keepalive signal to all subscribers.
    /// Default: 500 ms — must be well below <see cref="SubscriberHeartbeatTimeoutMs"/> (10 000 ms).
    /// Ratio is 20× so that several missed heartbeats are tolerated before the subscriber declares the publisher dead.
    /// </summary>
    public int PublisherHeartbeatIntervalMs { get; set; } = 500;

    public LeaderElectionSection LeaderElection { get; set; } = new();
    public StateSyncSection StateSync { get; set; } = new();
    public SubscriberSection Subscriber { get; set; } = new();

    public class LeaderElectionSection
    {
        /// <summary>
        /// Interval at which the current leader renews its KV lock. Default: 100 ms.
        /// Must be significantly lower than <see cref="DeadThresholdMs"/> (1 000 ms) so the leader
        /// can survive transient NATS round-trip delays without losing its lock.
        /// Ratio is 10× — provides headroom for up to 9 consecutive renewal failures.
        /// </summary>
        public int HeartbeatIntervalMs { get; set; } = 100;

        /// <summary>
        /// Duration without a successful leader heartbeat before followers attempt to elect a new leader.
        /// Default: 1 000 ms — aligns with the NATS KV TTL on the leader lock key so that
        /// the lock expires naturally if the leader dies, preventing indefinite lock holding.
        /// Lowering this below the NATS round-trip time will cause spurious failovers.
        /// </summary>
        public int DeadThresholdMs { get; set; } = 1000;
    }

    public class StateSyncSection
    {
        /// <summary>
        /// How often followers broadcast a full state snapshot to late-joining subscribers.
        /// Default: 15 000 ms — balances freshness against bandwidth; subscribers that connect
        /// mid-stream will receive a full image within at most one batch-sync window.
        /// Reduce if late-join latency is critical; increase for high-throughput streams.
        /// </summary>
        public int BatchSyncIntervalMs { get; set; } = 15_000;
    }

    public class SubscriberSection
    {
        /// <summary>
        /// How long a subscriber waits for the publisher to confirm receipt of its open-stream request.
        /// Default: 10 000 ms — covers worst-case startup contention where the publisher is still
        /// initialising or undergoing leader election. Exceeding this timeout throws <c>PublisherUnavailableException</c>.
        /// </summary>
        public int ConfirmationTimeoutMs { get; set; } = 10_000;

        /// <summary>
        /// Parallel dispatch workers per stream type. Default: processor count.
        /// Increase only if handlers are I/O-bound; for CPU-bound handlers keep at processor count
        /// to avoid context-switch overhead.
        /// </summary>
        public int DispatchWorkerCount { get; set; } = Environment.ProcessorCount;

        /// <summary>
        /// Capacity of the internal dispatch channel per stream (backpressure buffer). Default: 10 000.
        /// If the channel fills up, incoming messages are dropped — monitor queue depth under load
        /// and increase if drops are observed before scaling workers.
        /// </summary>
        public int DispatchChannelCapacity { get; set; } = 10_000;

        /// <summary>
        /// Maximum number of reconnect attempts before the subscriber gives up and throws. Default: 60.
        /// At the default <see cref="ReconnectInitialDelayMs"/> of 1 000 ms with exponential backoff
        /// this covers roughly 10 minutes of outage. Set to -1 for unlimited retries.
        /// </summary>
        public int MaxReconnectAttempts { get; set; } = 60;

        /// <summary>
        /// Delay before the first reconnect attempt. Default: 1 000 ms.
        /// Subsequent attempts use exponential backoff (capped internally) to avoid thundering-herd
        /// reconnection storms when a publisher restarts with many subscribers.
        /// </summary>
        public int ReconnectInitialDelayMs { get; set; } = 1_000;

        /// <summary>
        /// Duration without a publisher heartbeat before the subscriber triggers reconnection logic.
        /// Default: 1 500 ms — publisher sends keepalives every <see cref="StreamlySettings.PublisherHeartbeatIntervalMs"/> (500 ms),
        /// giving a 3× margin to absorb one or two dropped messages before acting.
        /// Must be lower than <see cref="StreamlySettings.SubscriberHeartbeatTimeoutMs"/> so the subscriber
        /// reacts before the publisher-side lease expires.
        /// </summary>
        public int HeartbeatTimeoutMs { get; set; } = 1_500;

        /// <summary>
        /// How often the subscriber sends a heartbeat to signal it is still alive. Default: 3 000 ms.
        /// The publisher-side timeout for subscriber liveness is <see cref="StreamlySettings.SubscriberHeartbeatTimeoutMs"/> (10 000 ms),
        /// giving a 3× margin — the subscriber can miss two consecutive beats before the publisher
        /// considers it gone and cleans up its slot.
        /// </summary>
        public int HeartbeatIntervalMs { get; set; } = 3_000;

        /// <summary>
        /// Maximum number of response messages buffered per request while waiting for publisher confirmation.
        /// Default: 50 — oldest entries are evicted once the limit is reached to guard against unbounded
        /// memory growth if a confirmation is lost. If eviction is observed in logs, either increase this
        /// limit or investigate confirmation delivery reliability.
        /// </summary>
        public int PendingResponseBufferLimit { get; set; } = 50;
    }
}
