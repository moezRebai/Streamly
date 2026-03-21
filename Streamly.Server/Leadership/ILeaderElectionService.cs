namespace Streamly.Server.Leadership;

/// <summary>
/// Manages leader election for a specific stream.
/// Each stream has its own independent leader election instance.
/// </summary>
public interface ILeaderElectionService : IDisposable
{
    string StreamName { get; }
    string InstanceId { get; }
    LeadershipState State { get; }
    bool IsLeader { get; }
    long CurrentEpoch { get; }
    string? CurrentLeaderId { get; }

    event EventHandler<LeadershipChangedEventArgs>? LeadershipChanged;

    Task<bool> TryAcquireLeadershipAsync(CancellationToken cancellationToken = default);
    Task StartAsync(CancellationToken cancellationToken = default);
    Task StopAsync(CancellationToken cancellationToken = default);
}
