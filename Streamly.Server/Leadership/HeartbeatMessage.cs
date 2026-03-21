using System.Text.Json.Serialization;

namespace Streamly.Server.Leadership;

/// <summary>
/// Heartbeat message published by leader to followers.
/// Internal coordination model - not exposed to users.
/// </summary>
internal class HeartbeatMessage
{
    [JsonPropertyName("leaderId")]
    public string LeaderId { get; set; } = string.Empty;

    [JsonPropertyName("epoch")]
    public long Epoch { get; set; }

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }

    [JsonPropertyName("streamName")]
    public string StreamName { get; set; } = string.Empty;

    [JsonPropertyName("activeRequestCount")]
    public int ActiveRequestCount { get; set; }
}
