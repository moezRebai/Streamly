using System.Text.Json.Serialization;

namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Heartbeat message published by leader to followers
/// Internal coordination model - not exposed to users
/// </summary>
internal class HeartbeatMessage
{
    /// <summary>
    /// Instance ID of the leader sending the heartbeat
    /// </summary>
    [JsonPropertyName("leaderId")]
    public string LeaderId { get; set; } = string.Empty;
    
    /// <summary>
    /// Current leadership epoch
    /// </summary>
    [JsonPropertyName("epoch")]
    public long Epoch { get; set; }
    
    /// <summary>
    /// Heartbeat timestamp (UTC)
    /// </summary>
    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }
    
    /// <summary>
    /// Stream name this heartbeat is for
    /// </summary>
    [JsonPropertyName("streamName")]
    public string StreamName { get; set; } = string.Empty;
    
    /// <summary>
    /// Optional: Number of active requests on this leader
    /// Can be used for health monitoring
    /// </summary>
    [JsonPropertyName("activeRequestCount")]
    public int ActiveRequestCount { get; set; }
}