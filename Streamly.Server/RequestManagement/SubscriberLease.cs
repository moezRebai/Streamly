namespace Streamly.Server.RequestManagement;

internal sealed class SubscriberLease
{
    public string SubscriberId { get; init; } = string.Empty;
    public DateTime LastHeartbeat { get; set; } = DateTime.UtcNow;
    public HashSet<string> RequestIds { get; } = new();
}
