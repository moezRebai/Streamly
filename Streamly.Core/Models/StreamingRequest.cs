namespace Streamly.Core.Models;

/// <summary>
/// Represents a streaming request with identity and lifecycle tracking
/// </summary>
/// <typeparam name="TRequest">The request payload type</typeparam>
public sealed class StreamingRequest<TRequest>
{
    /// <summary>
    /// Unique identifier for this request (SHA256 hash of payload)
    /// </summary>
    public required string RequestId { get; init; }
    
    /// <summary>
    /// The actual request payload (e.g., FX swap parameters)
    /// </summary>
    public required TRequest Payload { get; init; }
    
    /// <summary>
    /// When this request was first registered
    /// </summary>
    public required DateTimeOffset RegisteredAt { get; init; }
    
    /// <summary>
    /// Current lifecycle state
    /// </summary>
    public required RequestState State { get; set; }
    
    /// <summary>
    /// Number of active subscribers
    /// </summary>
    public int SubscriberCount { get; set; }
    
    /// <summary>
    /// When the last response was published (for staleness detection)
    /// </summary>
    public DateTimeOffset? LastPublishedAt { get; set; }
    
    /// <summary>
    /// Latest cached response for late subscribers
    /// </summary>
    public object? LatestImage { get; set; }
    
    /// <summary>
    /// Epoch when this request was last modified (for fencing)
    /// </summary>
    public long Epoch { get; set; }
}