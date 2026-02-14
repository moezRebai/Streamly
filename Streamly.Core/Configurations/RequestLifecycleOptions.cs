namespace Streamly.Core.Configurations;

/// <summary>
/// Request lifecycle configuration
/// </summary>
public sealed class RequestLifecycleOptions
{
    /// <summary>
    /// How often to call OnRequestUpdatedAsync (for price ticks)
    /// </summary>
    public TimeSpan UpdateInterval { get; set; } = TimeSpan.FromMilliseconds(100);
    
    /// <summary>
    /// Request timeout (no updates for this long = orphaned)
    /// </summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromMinutes(5);
    
    /// <summary>
    /// How long to keep requests in Closing state before removal
    /// </summary>
    public TimeSpan ClosingGracePeriod { get; set; } = TimeSpan.FromSeconds(2);
    
    /// <summary>
    /// Maximum cached image size per request (bytes)
    /// </summary>
    public int MaxImageSizeBytes { get; set; } = 5 * 1024; // 5KB
    
    /// <summary>
    /// Enable automatic cleanup of orphaned requests
    /// </summary>
    public bool EnableOrphanDetection { get; set; } = true;
    
    /// <summary>
    /// How often to scan for orphaned requests
    /// </summary>
    public TimeSpan OrphanScanInterval { get; set; } = TimeSpan.FromSeconds(30);
}