namespace Streamly.Audit;

/// <summary>
/// Configuration for the Streamly audit layer.
/// Bind from appsettings.json under "Streamly:Audit" or configure
/// inline via AddStreamlyAudit(options => { ... }).
/// </summary>
public sealed class AuditOptions
{
    /// <summary>Section name used when binding from IConfiguration.</summary>
    public const string SectionName = "Streamly:Audit";

    /// <summary>
    /// How long audit records are retained in the NATS JetStream stream.
    /// Records older than this are automatically discarded by NATS.
    /// Default: 14 days.
    /// </summary>
    public int RetentionDays { get; set; } = 14;

    /// <summary>
    /// Maximum total storage in gigabytes for the STREAMLY_AUDIT stream.
    /// When the cap is reached, the oldest records are discarded (DiscardPolicy.Old).
    /// Default: 10 GB.
    /// </summary>
    public int MaxStorageGb { get; set; } = 10;

    /// <summary>
    /// Capacity of the in-process Channel buffer between the publish hot path
    /// and the background drain loop that writes to JetStream.
    /// If the buffer is full (JetStream unavailable), new records are dropped
    /// and a warning is logged — the publish path is never blocked.
    /// Default: 10,000 records.
    /// </summary>
    public int SinkBufferCapacity { get; set; } = 10_000;

    /// <summary>
    /// Number of replicas for the STREAMLY_AUDIT JetStream stream.
    /// Set to 1 for single-node development, 3 for production HA clusters.
    /// Default: 1.
    /// </summary>
    public int StreamReplicas { get; set; } = 1;

    /// <summary>
    /// Whether to create the STREAMLY_AUDIT JetStream stream on startup
    /// if it does not already exist. The operation is idempotent — safe
    /// to call on every restart.
    /// Default: true.
    /// </summary>
    public bool CreateStreamOnStartup { get; set; } = true;

    /// <summary>
    /// Name of the JetStream stream used to store audit records.
    /// Change only if this name conflicts with an existing stream in your
    /// NATS cluster. All subjects under audit.{StreamName} are captured.
    /// Default: "STREAMLY_AUDIT".
    /// </summary>
    public string JetStreamName { get; set; } = "STREAMLY_AUDIT";

    /// <summary>
    /// Subject prefix for audit records.
    /// Each stream type publishes to audit.{StreamName} (e.g. audit.FxSpot).
    /// The JetStream stream captures all subjects matching audit.>
    /// Default: "audit".
    /// </summary>
    public string SubjectPrefix { get; set; } = "audit";
}