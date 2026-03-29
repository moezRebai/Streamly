namespace Streamly.Server.Leadership;

/// <summary>
/// Event arguments for leadership state changes.
/// </summary>
public class LeadershipChangedEventArgs(
    LeadershipState previousState,
    LeadershipState newState,
    string streamName,
    long epoch,
    string? leaderId = null)
    : EventArgs
{
    public LeadershipState PreviousState { get; } = previousState;
    public LeadershipState NewState { get; } = newState;
    public string StreamName { get; } = streamName ?? throw new ArgumentNullException(nameof(streamName));
    public long Epoch { get; } = epoch;
    public string? LeaderId { get; } = leaderId;
    public DateTime Timestamp { get; } = DateTime.UtcNow;
}
