using System.Text.Json.Serialization;

namespace Streamly.Server.Leadership;

/// <summary>
/// Value stored in Redis leader lock key.
/// Internal coordination model - not exposed to users.
/// </summary>
internal class LeaderLockValue
{
    [JsonPropertyName("instanceId")]
    public string InstanceId { get; set; } = string.Empty;

    [JsonPropertyName("epoch")]
    public long Epoch { get; set; }

    [JsonPropertyName("acquiredAt")]
    public DateTime AcquiredAt { get; set; }

    [JsonPropertyName("streamName")]
    public string StreamName { get; set; } = string.Empty;
}
