namespace Streamly.Core.Configurations;

/// <summary>
/// Client reconnection configuration
/// </summary>
public sealed class ReconnectOptions
{
    /// <summary>
    /// Initial retry delay
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(1);
    
    /// <summary>
    /// Maximum retry delay
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);
    
    /// <summary>
    /// Exponential backoff multiplier
    /// </summary>
    public double BackoffMultiplier { get; set; } = 2.0;
    
    /// <summary>
    /// Maximum reconnection attempts (0 = infinite)
    /// </summary>
    public int MaxAttempts { get; set; } = 10;
}