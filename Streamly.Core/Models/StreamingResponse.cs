namespace Streamly.Core.Models;

/// <summary>
/// Response published to subscribers
/// </summary>
/// <typeparam name="TResponse">The response payload type</typeparam>
public sealed class StreamingResponse<TResponse>
{
    /// <summary>
    /// Matches the request this responds to
    /// </summary>
    public required string RequestId { get; init; }
    
    /// <summary>
    /// Sequence number within this request stream
    /// </summary>
    public required long SequenceNumber { get; init; }
    
    /// <summary>
    /// The actual response data (e.g., FX pricing snapshot)
    /// </summary>
    public required TResponse Payload { get; init; }
    
    /// <summary>
    /// When this response was generated
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }
    
    /// <summary>
    /// True if this is the final response (stream closing)
    /// </summary>
    public bool IsFinal { get; init; }
    
    /// <summary>
    /// Optional close reason if IsFinal = true
    /// </summary>
    public CloseReason? CloseReason { get; init; }
    
    /// <summary>
    /// Optional error message if CloseReason = Error
    /// </summary>
    public string? ErrorMessage { get; init; }
}