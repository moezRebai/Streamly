using System.Text.Json.Serialization;
using Streamly.Core.Models;

namespace Streamly.Server.RequestManagement;

/// <summary>
/// Snapshot of a single active request.
/// </summary>
internal class ActiveRequestSnapshot
{
    [JsonPropertyName("requestId")]
    public string RequestId { get; set; } = string.Empty;

    [JsonPropertyName("serializedRequest")]
    public byte[] SerializedRequest { get; set; } = [];

    [JsonPropertyName("state")]
    public RequestState State { get; set; }

    [JsonPropertyName("subscriberCount")]
    public int SubscriberCount { get; set; }

    [JsonPropertyName("streamBehavior")]
    public StreamBehavior StreamBehavior { get; set; } = StreamBehavior.Live;

    [JsonPropertyName("openedAt")]
    public DateTime OpenedAt { get; set; }

    [JsonPropertyName("lastUpdateAt")]
    public DateTime? LastUpdateAt { get; set; }
}
