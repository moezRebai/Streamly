using System.Text.Json.Serialization;
using Streamly.Core.Models;

namespace Streamly.Server.Publishing;

/// <summary>
/// Published to streams.events channel when a request closes.
/// Received by all service instances to close locally.
/// </summary>
internal class RequestClosedEvent
{
    [JsonPropertyName("requestId")]
    public string RequestId { get; set; } = string.Empty;

    [JsonPropertyName("streamName")]
    public string StreamName { get; set; } = string.Empty;

    [JsonPropertyName("reason")]
    public CloseReason Reason { get; set; }

    [JsonPropertyName("epoch")]
    public long Epoch { get; set; }

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }
}
