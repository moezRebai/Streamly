namespace Streamly.Core.Models;

/// <summary>
/// Defines how a streaming request behaves
/// </summary>
public enum StreamBehavior
{
    /// <summary>
    /// Default - endless stream
    /// Closes when: subscriber count = 0, shutdown, error, timeout
    /// Provider never closes
    /// </summary>
    Live = 0,
    
    /// <summary>
    /// One response then auto-close
    /// Framework auto-closes after first PublishAsync
    /// Provider does not need to call CloseAsync
    /// </summary>
    Snapshot = 1
}