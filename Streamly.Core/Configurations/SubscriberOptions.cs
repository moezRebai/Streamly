namespace Streamly.Core.Configurations;

/// <summary>
/// Subscriber management configuration
/// </summary>
public sealed class SubscriberOptions
{
    /// <summary>
    /// Default timeout for detecting dead subscribers
    /// </summary>
    public TimeSpan SubscriberTimeout { get; set; } = TimeSpan.FromSeconds(30);
    
    /// <summary>
    /// Client-side heartbeat interval (must be < SubscriberTimeout)
    /// </summary>
    public TimeSpan ClientHeartbeatInterval { get; set; } = TimeSpan.FromSeconds(10);
    
    /// <summary>
    /// Automatically unsubscribe if subscriber times out
    /// </summary>
    public bool AutoUnsubscribeOnTimeout { get; set; } = true;
    
    /// <summary>
    /// Close request when last subscriber is removed
    /// </summary>
    public bool CloseOnLastUnsubscribe { get; set; } = true;
}