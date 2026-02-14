namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Manages leader election for a specific stream
/// Each stream has its own independent leader election instance
/// </summary>
public interface ILeaderElectionService : IDisposable
{
    /// <summary>
    /// Stream name this leader election manages
    /// </summary>
    string StreamName { get; }
    
    /// <summary>
    /// Unique identifier for this service instance
    /// </summary>
    string InstanceId { get; }
    
    /// <summary>
    /// Current leadership state
    /// </summary>
    LeadershipState State { get; }
    
    /// <summary>
    /// Whether this instance is currently the leader
    /// </summary>
    bool IsLeader { get; }
    
    /// <summary>
    /// Current leadership epoch
    /// Increments with each leadership change
    /// </summary>
    long CurrentEpoch { get; }
    
    /// <summary>
    /// Instance ID of the current leader (if known)
    /// Returns this instance's ID if this instance is leader
    /// </summary>
    string? CurrentLeaderId { get; }
    
    /// <summary>
    /// Event raised when leadership state changes
    /// </summary>
    event EventHandler<LeadershipChangedEventArgs>? LeadershipChanged;
    
    /// <summary>
    /// Try to acquire leadership for this stream
    /// Called by candidates when they detect no leader
    /// </summary>
    /// <returns>True if leadership acquired, false otherwise</returns>
    Task<bool> TryAcquireLeadershipAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Renew leadership lock (heartbeat to Redis)
    /// Called periodically by leader to maintain lock
    /// </summary>
    /// <returns>True if renewal successful, false if lost leadership</returns>
    Task<bool> RenewLeadershipAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Explicitly release leadership
    /// Called during graceful shutdown
    /// </summary>
    Task ReleaseLeadershipAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Start leader election service
    /// Begins monitoring for leadership opportunities
    /// </summary>
    Task StartAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Stop leader election service
    /// Releases leadership if held
    /// </summary>
    Task StopAsync(CancellationToken cancellationToken = default);
}