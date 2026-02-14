using System.Text.Json.Serialization;
using Streamly.Core.Models;

namespace Streamly.Core.Runtime.RequestManagement;

/// <summary>
/// Snapshot of a single active request
/// </summary>
internal class ActiveRequestSnapshot
{
    /// <summary>
    /// Request ID
    /// </summary>
    [JsonPropertyName("requestId")]
    public string RequestId { get; set; } = string.Empty;
    
    /// <summary>
    /// Serialized request payload (for recovery)
    /// </summary>
    [JsonPropertyName("serializedRequest")]
    public byte[] SerializedRequest { get; set; } = [];
    
    /// <summary>
    /// Current state
    /// </summary>
    [JsonPropertyName("state")]
    public RequestState State { get; set; }
    
    /// <summary>
    /// Number of active subscribers
    /// </summary>
    [JsonPropertyName("subscriberCount")]
    public int SubscriberCount { get; set; }
    
    [JsonPropertyName("streamBehavior")]        
    public StreamBehavior StreamBehavior { get; set; } = StreamBehavior.Live;

    /// <summary>
    /// When request was opened (UTC)
    /// </summary>
    [JsonPropertyName("openedAt")]
    public DateTime OpenedAt { get; set; }
    
    /// <summary>
    /// Last update time (UTC)
    /// </summary>
    [JsonPropertyName("lastUpdateAt")]
    public DateTime? LastUpdateAt { get; set; }
}