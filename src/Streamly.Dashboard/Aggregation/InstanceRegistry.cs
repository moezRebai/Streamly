using System.Collections.Concurrent;

namespace Streamly.Dashboard.Aggregation;

/// <summary>
/// Singleton registry that exposes the current <see cref="InstanceState"/> for every
/// configured Streamly instance.
///
/// Write path: <see cref="MetricsAggregator"/> updates states after each poll cycle.
/// Read path:  Blazor components and SignalR hub read states; they never mutate them.
/// </summary>
public sealed class InstanceRegistry
{
    private readonly ConcurrentDictionary<string, InstanceState> _states = new();

    /// <summary>
    /// Initialises the registry from the configured instance list.
    /// Called once at startup by <see cref="MetricsAggregator"/>.
    /// </summary>
    public void Initialise(IEnumerable<InstanceConfig> configs)
    {
        foreach (var cfg in configs)
        {
            _states[cfg.Id] = new InstanceState
            {
                Config       = cfg,
                IsReachable  = false,
                LastPolledAt = DateTimeOffset.UtcNow
            };
        }
    }

    /// <summary>Returns all instance states in configuration order.</summary>
    public IReadOnlyList<InstanceState> All =>
        _states.Values.ToList().AsReadOnly();

    /// <summary>Returns a single instance state, or null if not registered.</summary>
    public InstanceState? Get(string instanceId) =>
        _states.TryGetValue(instanceId, out var state) ? state : null;

    // ── Cluster-wide aggregates ────────────────────────────────────────────

    /// <summary>
    /// Total active streams across publisher and both-role instances only.
    /// Subscriber instances report the same streams as their publisher counterpart,
    /// so including them would double-count.
    /// </summary>
    public int TotalActiveStreams =>
        _states.Values
               .Where(s => s.IsReachable
                        && s.Metrics is not null
                        && s.Metrics.InstanceRole is "Publisher" or "Both")
               .Sum(s => s.Metrics!.ActiveStreams);

    /// <summary>
    /// Cluster-wide publish rate (sum of rolling 60s windows, publishers only).
    /// </summary>
    public double TotalPublishRatePerSec =>
        _states.Values
               .Where(s => s.IsReachable
                        && s.Metrics is not null
                        && s.Metrics.InstanceRole is "Publisher" or "Both")
               .Sum(s => s.Metrics!.PublishRatePerSec);

    /// <summary>
    /// Cluster-wide skip ratio: skipped / (published + skipped), publishers only.
    /// Returns 0 when no data is available.
    /// </summary>
    public double ClusterSkipRatio
    {
        get
        {
            long published = _states.Values
                .Where(s => s.IsReachable
                         && s.Metrics is not null
                         && s.Metrics.InstanceRole is "Publisher" or "Both")
                .Sum(s => s.Metrics!.TotalPublishes);

            long skipped = _states.Values
                .Where(s => s.IsReachable
                         && s.Metrics is not null
                         && s.Metrics.InstanceRole is "Publisher" or "Both")
                .Sum(s => s.Metrics!.TotalPublishSkips);

            long total = published + skipped;
            return total == 0 ? 0.0 : (double)skipped / total;
        }
    }

    /// <summary>Number of instances that are currently reachable and healthy.</summary>
    public int HealthyCount =>
        _states.Values.Count(s => s.Health == InstanceHealth.Healthy);

    /// <summary>Total number of configured instances.</summary>
    public int TotalCount => _states.Count;

    /// <summary>
    /// Returns the instance currently holding the leader lock, or null if none known.
    /// In a multi-stream-type deployment there may be multiple leaders (one per stream
    /// type); this returns the first one found — used only for the overview card.
    /// </summary>
    public InstanceState? Leader =>
        _states.Values.FirstOrDefault(s => s.IsReachable && s.IsLeader);

    // ── Internal write access ──────────────────────────────────────────────

    internal InstanceState GetOrCreate(string instanceId, InstanceConfig cfg) =>
        _states.GetOrAdd(instanceId, _ => new InstanceState
        {
            Config       = cfg,
            IsReachable  = false,
            LastPolledAt = DateTimeOffset.UtcNow
        });
}
