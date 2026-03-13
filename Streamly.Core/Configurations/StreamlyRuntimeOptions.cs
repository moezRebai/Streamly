namespace Streamly.Core.Configurations;

/// <summary>
/// Runtime configuration options.
/// Bound from appsettings.json "Streamly" section.
/// </summary>
public class StreamlyRuntimeOptions
{
    public const string SectionName = "Streamly";

    /// <summary>
    /// Logical name for this service.
    /// Used to build a human-readable InstanceId.
    /// Example: "SpotPricer", "FxSwapSubscriber", "PricingService"
    /// </summary>
    public string ServiceName { get; set; } = "StreamlyService";

    /// <summary>
    /// How long without a heartbeat before subscriber lease is considered expired.
    /// Default: 10s. Subscriber should send heartbeat every 3s.
    /// </summary>
    public int SubscriberHeartbeatTimeoutMs { get; set; } = 10000;
    
    /// <summary>
    /// How often leader publishes keepalive signal.
    /// Must be significantly less than subscriber HeartbeatTimeout.
    /// Default: 500ms — subscriber timeout is 2000ms = 4x margin
    /// </summary>
    public int PublisherHeartbeatIntervalMs { get; set; } = 500;
    
    /// <summary>
    /// Unique identifier for this service instance.
    /// If not explicitly set, defaults to "{ServiceName}-{MachineName}".
    /// Example: "SpotPricer-SERVER-01"
    /// 
    /// Must be unique across all running instances.
    /// Streamly will fail to start if another instance with the same
    /// InstanceId is already running.
    /// </summary>
    public string InstanceId
    {
        get => _instanceId ?? $"{ServiceName}-{Environment.MachineName}";
        set => _instanceId = value;
    }

    private string? _instanceId;
}