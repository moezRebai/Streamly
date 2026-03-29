using System.Text.Json.Serialization;

namespace Streamly.Client.Models;

/// <summary>
/// Lightweight preview for quick RequestId extraction
/// Avoids full deserialization in the Redis callback
/// </summary>
internal class ResponsePreview
{
    [JsonPropertyName("requestId")]
    public string RequestId { get; set; } = string.Empty;

    [JsonPropertyName("epoch")]
    public long Epoch { get; set; }

    [JsonPropertyName("isFinal")]
    public bool IsFinal { get; set; }
}
