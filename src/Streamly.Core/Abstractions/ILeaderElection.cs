namespace Streamly.Core.Abstractions;

/// <summary>
/// Transport-agnostic leader election abstraction.
/// Redis implementation: SET NX EX with a background heartbeat loop (~300 lines).
/// NATS implementation : JetStream KV with TTL auto-expire   (~20 lines).
/// </summary>
public interface ILeaderElection : IAsyncDisposable
{
    /// <summary>
    /// Attempt to acquire leadership.  Returns true if this instance became leader.
    /// Safe to call repeatedly - returns false if already leader or another instance holds the lock.
    /// </summary>
    Task<bool> TryAcquireLeadershipAsync(CancellationToken ct = default);

    /// <summary>
    /// Renew the leadership lease.  Must be called within the lease TTL.
    /// Throws if leadership has been lost (e.g. clock skew, network partition).
    /// </summary>
    Task RenewLeadershipAsync(CancellationToken ct = default);

    /// <summary>
    /// Voluntarily relinquish leadership (e.g. graceful shutdown).
    /// </summary>
    Task ReleaseLeadershipAsync(CancellationToken ct = default);

    /// <summary>True when this instance currently holds the leader lock.</summary>
    bool IsLeader { get; }

    /// <summary>
    /// Monotonically increasing fencing token.
    /// Incremented each time a new leader acquires the lock.
    /// Use this to reject stale messages from a former leader.
    /// </summary>
    int CurrentEpoch { get; }

    /// <summary>
    /// Raised whenever leadership changes.  Parameter is the new epoch number.
    /// Fired on both acquisition and loss of leadership.
    /// </summary>
    event Action<int>? OnLeadershipChanged;
}
