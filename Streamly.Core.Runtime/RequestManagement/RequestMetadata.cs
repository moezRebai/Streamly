using Streamly.Core.Models;

namespace Streamly.Core.Runtime.RequestManagement;

/// <summary>
/// Metadata for a single active streaming request
/// Stored in RequestRegistry for each active request
/// </summary>
internal class RequestMetadata<TRequest, TResponse>
{
    /// <summary>
    /// Unique identifier for this request (deterministic hash)
    /// </summary>
    public string RequestId { get; set; } = string.Empty;
    
    /// <summary>
    /// The original request object
    /// </summary>
    public TRequest Request { get; set; } = default!;
    
    /// <summary>
    /// Serialized request (for batch sync recovery)
    /// </summary>
    public byte[] SerializedRequest { get; set; } = [];
    
    /// <summary>
    /// Current state of the request
    /// </summary>
    public RequestState State { get; set; }
    
    /// <summary>
    /// Latest published response (for change detection)
    /// </summary>
    public TResponse? LatestImage { get; set; }
    
    /// <summary>
    /// Number of active subscribers
    /// </summary>
    public int SubscriberCount { get; set; }
    
    /// <summary>
    /// Stream behavior - Live (endless) or Snapshot (one response)
    /// </summary>
    public StreamBehavior StreamBehavior { get; set; } = StreamBehavior.Live; // ← ADDED

    /// <summary>
    /// When the request was opened (UTC)
    /// </summary>
    public DateTime OpenedAt { get; set; }
    
    /// <summary>
    /// When the request was last updated (UTC)
    /// </summary>
    public DateTime? LastUpdateAt { get; set; }
    
    /// <summary>
    /// Leadership epoch when request was opened
    /// Used for fencing stale updates
    /// </summary>
    public long Epoch { get; set; }
    
    /// <summary>
    /// Cancelled when request closes — stops the handler loop.
    /// </summary>
    public CancellationTokenSource HandlerCts { get; } = new();
}