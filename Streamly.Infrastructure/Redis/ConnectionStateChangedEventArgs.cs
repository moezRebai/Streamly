namespace Streamly.Infrastructure.Redis;

/// <summary>
/// Connection state change event arguments
/// </summary>
public class ConnectionStateChangedEventArgs(bool isConnected, string? reason = null) : EventArgs
{
    public bool IsConnected { get; } = isConnected;
    public string? Reason { get; } = reason;
    public DateTime Timestamp { get; } = DateTime.UtcNow;
}
