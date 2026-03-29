using System.Text.Json.Serialization;

namespace Streamly.Server.RequestManagement;

/// <summary>
/// Batch state synchronization message sent by leader.
/// </summary>
internal class BatchSyncMessage
{
    [JsonPropertyName("epoch")]
    public long Epoch { get; set; }

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }

    [JsonPropertyName("streamName")]
    public string StreamName { get; set; } = string.Empty;

    [JsonPropertyName("activeRequests")]
    public List<ActiveRequestSnapshot> ActiveRequests { get; set; } = new();

    [JsonPropertyName("chunkGroupId")]
    public string ChunkGroupId { get; set; } = string.Empty;

    [JsonPropertyName("chunkIndex")]
    public int ChunkIndex { get; set; }

    [JsonPropertyName("totalChunks")]
    public int TotalChunks { get; set; } = 1;
}
