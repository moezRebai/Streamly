namespace Streamly.Server.Leadership;

/// <summary>
/// Factory for creating leader election services per stream.
/// </summary>
public interface ILeaderElectionFactory
{
    /// <summary>
    /// Get or create a leader election service for a stream.
    /// Thread-safe, returns same instance for same stream name.
    /// </summary>
    Task<ILeaderElectionService> GetOrCreateAsync(string streamName, CancellationToken cancellationToken = default);
}
