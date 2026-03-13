namespace Streamly.Core.Runtime.RequestManagement;

/// <summary>
/// Tracks liveness of a single subscriber instance.
/// One lease per subscriberId covering ALL requestIds it consumes.
/// </summary>
internal sealed class SubscriberLease
{
    public string SubscriberId { get; init; } = string.Empty;
    public DateTime LastHeartbeat { get; set; } = DateTime.UtcNow;
    public HashSet<string> RequestIds { get; } = new();
}