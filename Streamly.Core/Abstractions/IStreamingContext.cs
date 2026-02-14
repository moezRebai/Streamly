using Streamly.Core.Models;

namespace Streamly.Core.Abstractions;

/// <summary>
/// Context provided to handler for publishing responses
/// </summary>
public interface IStreamingContext<in TResponse>
{
    /// <summary>
    /// Request ID for this streaming context
    /// </summary>
    string RequestId { get; }
    
    /// <summary>
    /// Stream behavior for this request
    /// Live: endless stream, framework manages close
    /// Snapshot: one response then auto-close
    /// </summary>
    StreamBehavior StreamBehavior { get; }
    
    /// <summary>
    /// Publish a response update
    /// Live: stays open after publish
    /// Snapshot: AUTO-CLOSES after first publish
    /// </summary>
    Task PublishAsync(
        TResponse response, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Close the stream explicitly (Live streams only)
    /// Snapshot streams auto-close after first publish
    /// </summary>
    Task CloseAsync(
        CloseReason reason = CloseReason.Normal,
        CancellationToken cancellationToken = default);
}