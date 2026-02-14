namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Represents the leadership state of a service instance for a specific stream
/// </summary>
public enum LeadershipState
{
    /// <summary>
    /// Instance is following another leader
    /// Processing requests but not publishing responses
    /// </summary>
    Follower = 0,
    
    /// <summary>
    /// Instance is attempting to acquire leadership
    /// Transitional state between Follower and Leader
    /// </summary>
    Candidate = 1,
    
    /// <summary>
    /// Instance is the leader
    /// Publishing responses and heartbeats
    /// </summary>
    Leader = 2
}