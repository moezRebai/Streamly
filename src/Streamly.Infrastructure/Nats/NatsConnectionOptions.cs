using System.ComponentModel.DataAnnotations;

namespace Streamly.Infrastructure.Nats;

/// <summary>
///     Configuration options for the NATS connection.
///     Bind from appsettings.json under the "Streamly:Nats" section.
/// </summary>
public sealed class NatsConnectionOptions
{
    /// <summary>
    ///     NATS server URL(s).  Supports comma-separated cluster URLs.
    ///     Examples:
    ///     "nats://localhost:4222"
    ///     "nats://10.0.0.1:4222,nats://10.0.0.2:4222,nats://10.0.0.3:4222"
    /// </summary>
    [Required]
    public string Url { get; set; } = "nats://localhost:4222";


    /// <summary>
    ///     Name passed to NATS server for connection identification (visible in monitoring).
    /// </summary>
    public string ConnectionName { get; set; } = "Streamly";

    /// <summary>
    ///     Maximum number of reconnect attempts before raising OnConnectionError.
    ///     -1 means unlimited (recommended for production).
    /// </summary>
    public int MaxReconnectAttempts { get; set; } = -1;

    /// <summary>
    ///     Delay between reconnect attempts.
    /// </summary>
    public TimeSpan ReconnectWait { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    ///     Optional username for NATS authentication.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    ///     Optional password for NATS authentication.
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    ///     Optional NKey seed or JWT credentials file path.
    /// </summary>
    public string? CredentialsFile { get; set; }

    // ── JetStream / Leader Election ───────────────────────────────────────────

    /// <summary>
    ///     TTL for the leader lock in the JetStream KV bucket.
    ///     Must be well above LeaderElection.HeartbeatIntervalMs to survive load-induced delays.
    ///     Effective failover time on leader crash = LeaderLockTtl (worst case).
    ///     Default: 3s.
    /// </summary>
    public TimeSpan LeaderLockTtl { get; set; } = TimeSpan.FromSeconds(3);
}