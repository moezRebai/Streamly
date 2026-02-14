namespace Streamly.Core.Runtime.Registration;

/// <summary>
/// Registry mapping request types to their configured stream names
/// </summary>
public interface IStreamRegistry
{
    /// <summary>
    /// Get the stream name for a request type
    /// </summary>
    /// <returns>Stream name if registered, throws otherwise</returns>
    string GetStreamName<TRequest>();
    
    /// <summary>
    /// Try to get the stream name for a request type
    /// </summary>
    bool TryGetStreamName<TRequest>(out string? streamName);
    
    /// <summary>
    /// Check if a request type is registered
    /// </summary>
    bool IsRegistered<TRequest>();
}