using System.Text.Json.Serialization;

namespace Streamly.Core.Models;

/// <summary>
/// Sent periodically by subscriber to signal it is still alive.
/// One message per subscriber instance covers all active streams.
/// </summary>
public class SubscriberHeartbeatMessage
{
    [JsonPropertyName("subscriberId")]
    public string SubscriberId { get; set; } = string.Empty;
}