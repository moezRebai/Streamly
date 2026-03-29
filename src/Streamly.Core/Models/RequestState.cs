namespace Streamly.Core.Models;

/// <summary>
/// Request lifecycle states
/// </summary>
public enum RequestState
{
    /// <summary>
    /// Request received, not yet streaming
    /// </summary>
    Registered = 0,
    
    /// <summary>
    /// Actively producing responses
    /// </summary>
    Streaming = 1,
    
    /// <summary>
    /// Close initiated, finishing current cycle
    /// </summary>
    Closing = 2,
    
    /// <summary>
    /// Closed and removed (ephemeral - exists only during transition)
    /// </summary>
    Closed = 3
}