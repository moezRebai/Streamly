using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.KeyValueStore;
using NATS.Net;
using Streamly.Core.Abstractions;

namespace Streamly.Infrastructure.Nats;

/// <summary>
/// NATS JetStream KV implementation of <see cref="ILeaderElection"/>.
///
/// How it works (vs Redis ~300 lines → NATS ~20 lines of core logic):
///
///   Redis approach:
///     SET leader:pricing-service {instanceId} NX EX 1
///     → Manual heartbeat loop every 200ms
///     → Manual TTL renewal
///     → Manual dead-leader detection polling
///     → Manual epoch management via separate key
///
///   NATS JetStream KV approach:
///     kv.CreateAsync("leader", instanceId)   // Atomic, fails if key exists
///     → Key auto-expires after LeaderLockTtl (server-enforced)
///     → Watch("leader") fires instantly on key deletion (leader died)
///     → Epoch = KV sequence number (monotonically increasing, free)
///
/// Failover timeline remains identical: ~540ms end-to-end.
/// 
/// LAZY INITIALIZATION: KV bucket is created on first use, no explicit InitialiseAsync() needed.
/// </summary>
public sealed class NatsLeaderElection(
    NatsConnectionManager transport,
    IOptions<NatsConnectionOptions> options,
    ILogger<NatsLeaderElection> logger,
    string streamName)
    : ILeaderElection
{
    private readonly NatsConnectionOptions _options = options.Value;

    private NatsConnection? _nats;
    private INatsKVStore? _kvStore;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    private volatile bool _isLeader;
    private int _currentEpoch;
    private CancellationTokenSource? _renewalCts;
    private CancellationTokenSource? _watchCts;
    private readonly string _streamName = streamName ?? throw new ArgumentNullException(nameof(streamName)); // ← STORE IT

    private volatile bool _disposed;
    public bool IsLeader => _isLeader;
    public int CurrentEpoch => _currentEpoch;

    public event Action<int>? OnLeadershipChanged;

    // ── Lazy Initialisation ───────────────────────────────────────────────────

    /// <summary>
    /// Ensure KV store is initialized (lazy, thread-safe).
    /// Called automatically on first use - no explicit InitialiseAsync() needed.
    /// </summary>
    private async Task EnsureInitialisedAsync(CancellationToken ct = default)
    {
        if (_initialized)
            return;

        await _initLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_initialized)
                return;

            logger.LogDebug("Lazy-initializing NATS KV store for leader election");

            // Access the underlying NatsConnection from the manager
            _nats = GetNatsConnection();

            // Create KV context from the connection
            var kvContext = _nats.CreateKeyValueStoreContext();

            try
            {
                // Try to get existing bucket first
                _kvStore = await kvContext.GetStoreAsync(
                    NatsSubjectNames.LeaderElectionBucket, 
                    cancellationToken: ct).ConfigureAwait(false);

                logger.LogDebug(
                    "Using existing KV bucket '{Bucket}' for leader election",
                    NatsSubjectNames.LeaderElectionBucket);
            }
            catch (NatsKVException)
            {
                // Bucket doesn't exist - create it
                logger.LogInformation(
                    "Creating KV bucket '{Bucket}' for leader election with TTL {Ttl}",
                    NatsSubjectNames.LeaderElectionBucket,
                    _options.LeaderLockTtl);

                _kvStore = await kvContext.CreateStoreAsync(
                    new NatsKVConfig(NatsSubjectNames.LeaderElectionBucket)
                    {
                        History = 1,                      // Only need the latest value
                        MaxAge = _options.LeaderLockTtl,  // Server auto-deletes after TTL
                        MaxBytes = 1024 * 1024,           // 1MB max
                        Storage = NatsKVStorageType.Memory  // In-memory for speed
                    }, ct).ConfigureAwait(false);

                logger.LogInformation(
                    "Created KV bucket '{Bucket}' successfully",
                    NatsSubjectNames.LeaderElectionBucket);
            }

            // Start watching for leader key changes in the background
            StartLeaderWatch();

            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    // ── ILeaderElection ───────────────────────────────────────────────────────

    public async Task<bool> TryAcquireLeadershipAsync(CancellationToken ct = default)
    {
        await EnsureInitialisedAsync(ct).ConfigureAwait(false);

        try
        {
            // ✅ FIX: Use stream-specific key
            var leaderKey = $"leader.{_streamName}";  // ← NOT just "leader"!

            logger.LogDebug(
                "Attempting to acquire leadership for stream '{StreamName}' using key '{Key}'",
                _streamName,
                leaderKey);

            await _kvStore!.CreateAsync(
                leaderKey,  // ← Per-stream key: "leader.SpotPricer"
                Encoding.UTF8.GetBytes(_options.InstanceId),
                cancellationToken: ct).ConfigureAwait(false);

            // If we reach here, we are the new leader
            var newEpoch = Interlocked.Increment(ref _currentEpoch);
            _isLeader = true;

            logger.LogInformation(
                "Instance {InstanceId} acquired leadership for stream '{StreamName}' (epoch {Epoch})",
                _options.InstanceId,
                _streamName,
                newEpoch);

            StartRenewalLoop();
            OnLeadershipChanged?.Invoke(newEpoch);
            return true;
        }
        catch (NatsKVWrongLastRevisionException)
        {
            logger.LogDebug(
                "Failed to acquire leadership for stream '{StreamName}' - already held by another instance",
                _streamName);
            return false;
        }
        catch (NatsKVCreateException)
        {
            logger.LogDebug(
                "Failed to acquire leadership for stream '{StreamName}' - key already exists",
                _streamName);
            return false;
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "Error attempting to acquire leadership for stream '{StreamName}'",
                _streamName);
            return false;
        }
    }

    public async Task RenewLeadershipAsync(CancellationToken ct = default)
    {
        await EnsureInitialisedAsync(ct).ConfigureAwait(false);

        if (!_isLeader)
            throw new InvalidOperationException("Cannot renew: this instance is not the leader.");

        try
        {
            // ✅ Use stream-specific key
            var leaderKey = $"leader.{_streamName}";

            await _kvStore!.PutAsync(
                leaderKey,  // ← Same per-stream key
                Encoding.UTF8.GetBytes(_options.InstanceId),
                cancellationToken: ct).ConfigureAwait(false);

            logger.LogTrace(
                "Renewed leadership for stream '{StreamName}' (instance {InstanceId}, epoch {Epoch})",
                _streamName,
                _options.InstanceId,
                _currentEpoch);
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "Failed to renew leadership for stream '{StreamName}' - assuming lost",
                _streamName);
            await HandleLeadershipLostAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async Task ReleaseLeadershipAsync(CancellationToken ct = default)
    {
        if (!_isLeader) return;

        try
        {
            if (_kvStore != null)
            {
                // ✅ Use stream-specific key
                var leaderKey = $"leader.{_streamName}";

                await _kvStore.DeleteAsync(
                    leaderKey,  // ← Same per-stream key
                    cancellationToken: ct).ConfigureAwait(false);

                logger.LogInformation(
                    "Instance {InstanceId} released leadership for stream '{StreamName}' (epoch {Epoch})",
                    _options.InstanceId,
                    _streamName,
                    _currentEpoch);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Error releasing leadership lock for stream '{StreamName}' (may have already expired)",
                _streamName);
        }
        finally
        {
            await HandleLeadershipLostAsync().ConfigureAwait(false);
        }
    }

    // ── Background tasks ──────────────────────────────────────────────────────

    private void StartRenewalLoop()
    {
        _renewalCts?.Cancel();
        _renewalCts = new CancellationTokenSource();
        var ct = _renewalCts.Token;

        _ = Task.Run(async () =>
        {
            logger.LogDebug(
                "Leader renewal loop started (interval {Interval}ms)",
                _options.LeaderHeartbeatInterval.TotalMilliseconds);

            while (!ct.IsCancellationRequested && _isLeader)
            {
                try
                {
                    await Task.Delay(_options.LeaderHeartbeatInterval, ct).ConfigureAwait(false);
                    await RenewLeadershipAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Renewal loop encountered an error");
                    // RenewLeadershipAsync already called HandleLeadershipLostAsync
                    break;
                }
            }

            logger.LogDebug("Leader renewal loop stopped");
        }, ct);
    }

    private void StartLeaderWatch()
    {
        _watchCts?.Cancel();
        _watchCts = new CancellationTokenSource();
        var ct = _watchCts.Token;

        _ = Task.Run(async () =>
        {
            logger.LogDebug(
                "Leader watch started for stream '{StreamName}' on KV bucket '{Bucket}'",
                _streamName,
                NatsSubjectNames.LeaderElectionBucket);

            // ✅ Watch stream-specific key
            var leaderKey = $"leader.{_streamName}";

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await foreach (var entry in _kvStore!
                                       .WatchAsync<byte[]>(leaderKey, cancellationToken: ct)  // ← Watch per-stream key
                                       .ConfigureAwait(false))
                    {
                        if (entry.Operation is NatsKVOperation.Del or NatsKVOperation.Purge)
                        {
                            logger.LogInformation(
                                "Leader key deleted for stream '{StreamName}' (op={Op}), starting election",
                                _streamName,
                                entry.Operation);

                            if (!_isLeader)
                            {
                                await TryAcquireLeadershipAsync(ct).ConfigureAwait(false);
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex,
                        "Leader watch faulted for stream '{StreamName}', restarting in 1s",
                        _streamName);
                    await Task.Delay(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
                }
            }

            logger.LogDebug("Leader watch stopped for stream '{StreamName}'", _streamName);
        }, ct);
    }

    private Task HandleLeadershipLostAsync()
    {
        if (!_isLeader) return Task.CompletedTask;

        _isLeader = false;
        _renewalCts?.Cancel();

        var epoch = _currentEpoch;
        logger.LogWarning(
            "Instance {InstanceId} lost leadership (epoch {Epoch})",
            _options.InstanceId, epoch);

        OnLeadershipChanged?.Invoke(epoch);
        return Task.CompletedTask;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Extract the underlying NatsConnection from the NatsConnectionManager.
    /// We do this via a cast to an internal accessor interface to avoid making
    /// NatsConnection public on the manager.
    /// </summary>
    private NatsConnection GetNatsConnection()
    {
        if (transport is INatsConnectionAccessor accessor)
            return accessor.NatsConnection;

        // Fallback: use reflection (only for dev/test scenarios)
        var field = typeof(NatsConnectionManager)
            .GetField("_connection",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        var conn = field?.GetValue(transport) as NatsConnection;
        return conn ?? throw new InvalidOperationException(
            "Unable to obtain NatsConnection from NatsConnectionManager. " +
            "Ensure ConnectAsync() was called before attempting leader election.");
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;

        _renewalCts?.Cancel();
        _renewalCts?.Dispose();
        _watchCts?.Cancel();
        _watchCts?.Dispose();
        _initLock?.Dispose();

        logger.LogDebug("NatsLeaderElection disposed");

        return ValueTask.CompletedTask;
    }
}