namespace Streamly.Core.Models;

/// <summary>
/// Reasons for request closure
/// </summary>
public enum CloseReason
{
    /// <summary>
    /// Normal completion
    /// </summary>
    Normal = 0,
    
    /// <summary>
    /// All subscribers unsubscribed
    /// </summary>
    Unsubscribe = 1,
    
    /// <summary>
    /// Handler encountered an error
    /// </summary>
    Error = 2,
    
    /// <summary>
    /// Request timed out
    /// </summary>
    Timeout = 3,
    
    /// <summary>
    /// No subscribers detected (client crashed)
    /// </summary>
    Orphaned = 4,
    
    /// <summary>
    /// Service shutting down
    /// </summary>
    Shutdown = 5,

    /// <summary>
    /// No cluster accepted the request — all clusters either filtered it out or are offline.
    /// </summary>
    NoProvider = 6
}