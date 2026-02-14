namespace Streamly.Core.Runtime.Configuration;

/// <summary>
/// Leader election configuration (bound from appsettings.json)
/// </summary>
public class LeaderElectionOptions
{
    public const string SectionName = "Streamly:LeaderElection";
    
    /// <summary>
    /// Heartbeat interval (default: 200ms)
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromMilliseconds(200);
    
    /// <summary>
    /// Leader lock TTL in Redis (default: 1 second)
    /// </summary>
    public TimeSpan LockTtl { get; set; } = TimeSpan.FromSeconds(1);
    
    /// <summary>
    /// Dead leader threshold - no heartbeat for this duration = dead (default: 500ms)
    /// </summary>
    public TimeSpan DeadThreshold { get; set; } = TimeSpan.FromMilliseconds(500);
}