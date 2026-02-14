namespace Streamly.Core.Configurations;

/// <summary>
/// Leader election configuration
/// </summary>
public sealed class LeaderElectionOptions
{
    /// <summary>
    /// How often leader sends heartbeats
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromMilliseconds(200);
    
    /// <summary>
    /// When to consider leader dead (no heartbeat)
    /// </summary>
    public TimeSpan DeadThreshold { get; set; } = TimeSpan.FromMilliseconds(500);
    
    /// <summary>
    /// TTL for Redis leader lock
    /// </summary>
    public TimeSpan LockTtl { get; set; } = TimeSpan.FromSeconds(1);
    
    /// <summary>
    /// How often followers check for leadership
    /// </summary>
    public TimeSpan CheckInterval { get; set; } = TimeSpan.FromMilliseconds(100);
    
    /// <summary>
    /// Max attempts to acquire leadership
    /// </summary>
    public int MaxAcquisitionAttempts { get; set; } = 3;
}