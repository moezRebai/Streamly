namespace Streamly.Core.Configurations;

/// <summary>
/// Client-side connection configuration
/// </summary>
public sealed class StreamlyClientOptions
{
    /// <summary>
    /// Redis connection string
    /// </summary>
    public required string RedisConnectionString { get; set; }
    
    /// <summary>
    /// Service name to connect to
    /// </summary>
    public required string ServiceName { get; set; }
    
    /// <summary>
    /// Unique client identifier
    /// </summary>
    public string ClientId { get; set; } = Guid.NewGuid().ToString("N");
    
    /// <summary>
    /// Timeout for leader heartbeat detection (must be > 1 second failover)
    /// </summary>
    public TimeSpan HeartbeatTimeout { get; set; } = TimeSpan.FromSeconds(5);
    
    /// <summary>
    /// Reconnect strategy
    /// </summary>
    public ReconnectOptions Reconnect { get; set; } = new();
}