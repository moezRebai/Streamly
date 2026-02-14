namespace Streamly.Core.Configurations;

/// <summary>
/// Configuration for the Streamly distributed streaming library
/// </summary>
public sealed class StreamlyOptions
{
    /// <summary>
    /// Redis connection string
    /// </summary>
    public required string RedisConnectionString { get; set; }
    
    /// <summary>
    /// Unique identifier for this service instance
    /// </summary>
    public string InstanceId { get; set; } = Guid.NewGuid().ToString("N");
    
    /// <summary>
    /// Service name (for channel prefixes)
    /// </summary>
    public required string ServiceName { get; set; }
    
    /// <summary>
    /// Leader election settings
    /// </summary>
    public LeaderElectionOptions LeaderElection { get; set; } = new();
    
    /// <summary>
    /// Request lifecycle settings
    /// </summary>
    public RequestLifecycleOptions RequestLifecycle { get; set; } = new();
    
    /// <summary>
    /// Subscriber management settings
    /// </summary>
    public SubscriberOptions Subscriber { get; set; } = new();
    
    /// <summary>
    /// Failover and recovery settings
    /// </summary>
    public FailoverOptions Failover { get; set; } = new();
}