namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Event arguments for leadership state changes
/// </summary>
public class LeadershipChangedEventArgs(
    LeadershipState previousState,
    LeadershipState newState,
    string streamName,
    long epoch,
    string? leaderId = null)
    : EventArgs
{
    /// <summary>
    /// Previous leadership state
    /// </summary>
    public LeadershipState PreviousState { get; } = previousState;

    /// <summary>
    /// New leadership state
    /// </summary>
    public LeadershipState NewState { get; } = newState;

    /// <summary>
    /// Stream name this leadership change applies to
    /// </summary>
    public string StreamName { get; } = streamName ?? throw new ArgumentNullException(nameof(streamName));

    /// <summary>
    /// Current epoch after the change
    /// </summary>
    public long Epoch { get; } = epoch;

    /// <summary>
    /// Instance ID of the new leader (if known)
    /// </summary>
    public string? LeaderId { get; } = leaderId;

    /// <summary>
    /// Timestamp of the leadership change
    /// </summary>
    public DateTime Timestamp { get; } = DateTime.UtcNow;
}