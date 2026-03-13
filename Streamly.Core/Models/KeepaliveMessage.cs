using System.Text.Json.Serialization;

namespace Streamly.Core.Models;

public class KeepaliveMessage
{
    [JsonPropertyName("streamName")]
    public string StreamName { get; set; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }
}