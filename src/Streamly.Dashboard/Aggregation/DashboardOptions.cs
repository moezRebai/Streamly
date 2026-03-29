namespace Streamly.Dashboard.Aggregation;

/// <summary>
/// Strongly-typed configuration for the dashboard, bound from
/// "Streamly:Dashboard" in appsettings.json.
/// </summary>
public sealed class DashboardOptions
{
    public const string SectionKey = "Streamly:Dashboard";

    /// <summary>
    /// How often (in milliseconds) the MetricsAggregator polls each instance.
    /// Default: 2000ms.
    /// </summary>
    public int PollIntervalMs { get; set; } = 2000;

    /// <summary>
    /// The list of Streamly service instances the dashboard should monitor.
    /// </summary>
    public List<InstanceConfig> Instances { get; set; } = [];
}

/// <summary>
/// Configuration entry for a single Streamly service instance.
/// </summary>
public sealed class InstanceConfig
{
    /// <summary>
    /// Stable unique identifier for this instance (e.g. "pub-1").
    /// Used as the dictionary key throughout the dashboard.
    /// </summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Base URL of the instance's monitoring HTTP server,
    /// e.g. "http://pricing-svc-1:5001".
    /// The dashboard appends /streamly/metrics, /streamly/streams, etc.
    /// </summary>
    public string BaseUrl { get; set; } = string.Empty;

    /// <summary>
    /// Human-readable label shown in the UI (e.g. "Publisher 1").
    /// </summary>
    public string Label { get; set; } = string.Empty;
}
