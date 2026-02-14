using System.Text.Json.Serialization;

namespace Streamly.Core.Runtime.RequestManagement;

/// <summary>
/// Batch state synchronization message sent by leader
/// Published to streams.batch.{streamName} every 15 seconds
/// </summary>
internal class BatchSyncMessage
{
    /// <summary>
    /// Current leadership epoch
    /// </summary>
    [JsonPropertyName("epoch")]
    public long Epoch { get; set; }
    
    /// <summary>
    /// When this batch was created (UTC)
    /// </summary>
    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }
    
    /// <summary>
    /// Stream name this batch is for
    /// </summary>
    [JsonPropertyName("streamName")]
    public string StreamName { get; set; } = string.Empty;
    
    /// <summary>
    /// Snapshot of all active requests
    /// </summary>
    [JsonPropertyName("activeRequests")]
    public List<ActiveRequestSnapshot> ActiveRequests { get; set; } = new();
}