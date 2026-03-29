namespace Streamly.Monitoring;

/// <summary>
/// Configuration for the Streamly monitoring layer.
/// Bind from appsettings.json under "Streamly:Monitoring" or configure
/// inline via AddStreamlyMonitoring(options => { ... }).
/// </summary>
public sealed class MonitoringOptions
{
    /// <summary>Section name used when binding from IConfiguration.</summary>
    public const string SectionName = "Streamly:Monitoring";

    /// <summary>
    /// Unique identifier for this instance shown in all snapshots.
    /// Defaults to the machine name. Override when running multiple
    /// instances on the same host (e.g. set via environment variable).
    /// </summary>
    public string InstanceId { get; set; } = Environment.MachineName;

    /// <summary>
    /// Role label shown in snapshots and on the dashboard instance table.
    /// Typical values: "Publisher", "Subscriber", "Both".
    /// Set automatically by AddStreamlyMonitoring() when called after
    /// AddStreamlyServer() or AddStreamlyClient().
    /// </summary>
    public string InstanceRole { get; set; } = "Unknown";

    /// <summary>
    /// How long to retain stream records for closed streams before removing
    /// them from the in-memory table. Closed streams are kept briefly so
    /// the dashboard can show recent closes without them disappearing instantly.
    /// Default: 5 minutes.
    /// </summary>
    public TimeSpan RetainClosedStreamsFor { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Window used to compute the rolling publish rate (publishes/second).
    /// The collector counts publishes within this window and divides by the
    /// window duration. Default: 60 seconds.
    /// </summary>
    public TimeSpan PublishRateWindow { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Route prefix for all monitoring endpoints.
    /// Default: "/streamly" → endpoints are /streamly/health,
    /// /streamly/metrics, /streamly/streams, /streamly/streams/{id}.
    /// Change only if this prefix conflicts with existing routes.
    /// </summary>
    public string RoutePrefix { get; set; } = "/streamly";
}
