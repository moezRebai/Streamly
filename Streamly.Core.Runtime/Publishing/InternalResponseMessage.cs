using System.Text.Json.Serialization;
using Streamly.Core.Models;

namespace Streamly.Core.Runtime.Publishing;

/// <summary>
/// Internal Redis message wrapping a response
/// Contains epoch for split-brain protection
/// NOT exposed to users - epoch stripped before delivery
/// </summary>
internal class InternalResponseMessage<TResponse>
{
    /// <summary>
    /// Request ID this response belongs to
    /// </summary>
    [JsonPropertyName("requestId")]
    public string RequestId { get; set; } = string.Empty;

    /// <summary>
    /// The actual response data
    /// </summary>
    [JsonPropertyName("data")]
    public TResponse Data { get; set; } = default!;

    /// <summary>
    /// Leadership epoch when published (for fencing)
    /// </summary>
    [JsonPropertyName("epoch")]
    public long Epoch { get; set; }

    /// <summary>
    /// Publisher instance ID (for debugging)
    /// </summary>
    [JsonPropertyName("publisherId")]
    public string PublisherId { get; set; } = string.Empty;

    /// <summary>
    /// When published (UTC)
    /// </summary>
    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }

    /// <summary>
    /// Whether this is the final response (request closing)
    /// </summary>
    [JsonPropertyName("isFinal")]
    public bool IsFinal { get; set; }

    /// <summary>
    /// Close reason (only set when IsFinal = true)
    /// </summary>
    [JsonPropertyName("closeReason")]
    public CloseReason? CloseReason { get; set; }
}