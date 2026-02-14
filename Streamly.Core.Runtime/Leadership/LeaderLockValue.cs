using System.Text.Json.Serialization;

namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Value stored in Redis leader lock key
/// Internal coordination model - not exposed to users
/// </summary>
internal class LeaderLockValue
{
    /// <summary>
    /// Unique identifier of the instance holding the lock
    /// </summary>
    [JsonPropertyName("instanceId")]
    public string InstanceId { get; set; } = string.Empty;
    
    /// <summary>
    /// Leadership epoch (generation number)
    /// Incremented on each leadership change for fencing
    /// </summary>
    [JsonPropertyName("epoch")]
    public long Epoch { get; set; }
    
    /// <summary>
    /// When the lock was acquired (UTC)
    /// </summary>
    [JsonPropertyName("acquiredAt")]
    public DateTime AcquiredAt { get; set; }
    
    /// <summary>
    /// Stream name this lock is for
    /// </summary>
    [JsonPropertyName("streamName")]
    public string StreamName { get; set; } = string.Empty;
}