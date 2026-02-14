namespace Streamly.Core.Configurations;

/// <summary>
/// Failover and recovery configuration
/// </summary>
public sealed class FailoverOptions
{
    /// <summary>
    /// Enable failover recovery (republish images)
    /// </summary>
    public bool EnableFailoverRecovery { get; set; } = true;
    
    /// <summary>
    /// Max time to complete promotion after detecting leader failure
    /// </summary>
    public TimeSpan MaxPromotionTime { get; set; } = TimeSpan.FromMilliseconds(100);
    
    /// <summary>
    /// Republish high-priority images immediately after promotion
    /// </summary>
    public bool RepublishOnPromotion { get; set; } = true;
    
    /// <summary>
    /// Max requests to republish during failover (priority-based)
    /// </summary>
    public int MaxFailoverRepublishCount { get; set; } = 1000;
    
    /// <summary>
    /// State synchronization interval (batch sync)
    /// </summary>
    public TimeSpan StateSyncInterval { get; set; } = TimeSpan.FromSeconds(15);
}