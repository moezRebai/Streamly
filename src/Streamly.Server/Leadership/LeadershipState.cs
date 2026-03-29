namespace Streamly.Server.Leadership;

/// <summary>
/// Represents the leadership state of a service instance for a specific stream.
/// </summary>
public enum LeadershipState
{
    Follower = 0,
    Candidate = 1,
    Leader = 2
}
