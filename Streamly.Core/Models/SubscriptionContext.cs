namespace Streamly.Core.Models;

/// <summary>
/// Subscription context for client-side operations
/// </summary>
public sealed class SubscriptionContext
{
    /// <summary>
    /// Request being subscribed to
    /// </summary>
    public required string RequestId { get; init; }
    
    /// <summary>
    /// Client identifier (for tracking subscribers)
    /// </summary>
    public required string SubscriberId { get; init; }
    
    /// <summary>
    /// When subscription was established
    /// </summary>
    public required DateTimeOffset SubscribedAt { get; init; }
    
    /// <summary>
    /// Timeout for detecting dead subscribers
    /// </summary>
    public TimeSpan SubscriberTimeout { get; init; } = TimeSpan.FromSeconds(30);
}