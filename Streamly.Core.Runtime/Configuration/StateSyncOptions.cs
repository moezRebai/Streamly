namespace Streamly.Core.Runtime.Configuration;

/// <summary>
/// Configuration for state synchronization
/// </summary>
public class StateSyncOptions
{
    public const string SectionName = "Streamly:StateSync";
    
    /// <summary>
    /// Interval for batch sync publishing (default: 15 seconds)
    /// </summary>
    public TimeSpan BatchSyncInterval { get; set; } = TimeSpan.FromSeconds(15);
    
    /// <summary>
    /// Maximum requests per batch (for pagination, not implemented yet)
    /// </summary>
    public int MaxRequestsPerBatch { get; set; } = 1000;
    
    /// <summary>
    /// Whether to compress batch sync messages (not implemented yet)
    /// </summary>
    public bool CompressBatchSync { get; set; } = false;
}