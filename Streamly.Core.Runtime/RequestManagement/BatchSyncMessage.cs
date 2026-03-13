using System.Text.Json.Serialization;

namespace Streamly.Core.Runtime.RequestManagement;

/// <summary>
/// Batch state synchronization message sent by leader.
/// Published to streams.batch.{streamName} every 15 seconds.
///
/// Large registries are split into multiple chunks to stay under NATS's
/// max payload limit (1 MB default). All chunks in a group share the same
/// ChunkGroupId. A receiver must accumulate all TotalChunks before reconciling.
///
/// Single-chunk batches (TotalChunks == 1) are the common case at low stream
/// counts and are processed immediately without any accumulation.
/// </summary>
internal class BatchSyncMessage
{
    /// <summary>Current leadership epoch.</summary>
    [JsonPropertyName("epoch")]
    public long Epoch { get; set; }

    /// <summary>When this batch was created (UTC).</summary>
    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }

    /// <summary>Stream name this batch is for.</summary>
    [JsonPropertyName("streamName")]
    public string StreamName { get; set; } = string.Empty;

    /// <summary>Snapshot of active requests carried by this chunk.</summary>
    [JsonPropertyName("activeRequests")]
    public List<ActiveRequestSnapshot> ActiveRequests { get; set; } = new();

    // ── chunk coordination ────────────────────────────────────────────────────

    /// <summary>
    /// Unique id shared by every chunk belonging to the same batch publish.
    /// Generated once per PublishBatchSyncAsync call (Guid.NewGuid).
    /// Empty string on legacy single-message batches (treated as TotalChunks=1).
    /// </summary>
    [JsonPropertyName("chunkGroupId")]
    public string ChunkGroupId { get; set; } = string.Empty;

    /// <summary>0-based position of this chunk within the group.</summary>
    [JsonPropertyName("chunkIndex")]
    public int ChunkIndex { get; set; }

    /// <summary>
    /// Total number of chunks in this group.
    /// Defaults to 1 so that legacy messages (field absent / zero) are handled
    /// correctly by the receiver without any special-casing.
    /// </summary>
    [JsonPropertyName("totalChunks")]
    public int TotalChunks { get; set; } = 1;
}