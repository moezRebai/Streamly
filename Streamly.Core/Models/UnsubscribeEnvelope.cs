using System.Text.Json.Serialization;

namespace Streamly.Core.Models;

/// <summary>
/// Published to Redis when client unsubscribes
/// Leader decrements subscriber count
/// Auto-closes stream if count reaches 0 (Live only)
/// </summary>
public class UnsubscribeEnvelope
{
    /// <summary>
    /// Request ID to unsubscribe from
    /// </summary>
    [JsonPropertyName("requestId")]
    public string RequestId { get; set; } = string.Empty;
    
    /// <summary>
    /// Stream name (for routing)
    /// </summary>
    [JsonPropertyName("streamName")]
    public string StreamName { get; set; } = string.Empty;
    
    /// <summary>
    /// When unsubscribe was requested (UTC)
    /// </summary>
    [JsonPropertyName("unsubscribedAt")]
    public DateTime UnsubscribedAt { get; set; } = DateTime.UtcNow;
}