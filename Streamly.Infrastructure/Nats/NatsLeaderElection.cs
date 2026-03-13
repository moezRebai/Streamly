using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.KeyValueStore;
using NATS.Net;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;

namespace Streamly.Infrastructure.Nats;

/// <summary>
/// NATS JetStream KV implementation of leader election with split-brain prevention.
///
/// Key guarantees:
///   - Only one instance can hold leadership at a time (KV CreateAsync atomicity)
///   - Leadership renewal uses optimistic concurrency (UpdateAsync with revision check)
///     so a paused-then-resumed ex-leader cannot silently overwrite a new leader
///   - Self-demotion only occurs on confirmed loss (WrongLastRevision), not transient errors
///   - Followers add random jitter before election attempt to reduce thundering herd
/// </summary>
public sealed class NatsLeaderElection : ILeaderElection
{
    private readonly NatsConnectionManager _transport;
    private readonly IOptions<StreamlyRuntimeOptions> _runtimeOptions;
    private readonly ISubjectResolver _subjectResolver;
    private readonly NatsConnectionOptions _options;
    private readonly ILogger<NatsLeaderElection> _logger;
    private readonly string _streamName;
    private readonly string _leaderKey;

    private NatsConnection? _nats;
    private INatsKVStore? _kvStore;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    // Tracks the KV revision of our current leadership entry.
    // UpdateAsync requires the exact last revision — this is what prevents split-brain.
    private ulong _leaderRevision;

    private volatile bool _isLeader;
    private int _currentEpoch;
    private CancellationTokenSource? _renewalCts;
    private CancellationTokenSource? _watchCts;
    private volatile bool _disposed;

    // How many consecutive renewal failures before we self-demote.
    // This prevents transient NATS hiccups from causing unnecessary leader churn.
    private const int MaxConsecutiveRenewalFailures = 3;

    public bool IsLeader => _isLeader;
    public int CurrentEpoch => _currentEpoch;
    public event Action<int>? OnLeadershipChanged;

    public NatsLeaderElection(
        NatsConnectionManager transport,
        IOptions<NatsConnectionOptions> options,
        IOptions<StreamlyRuntimeOptions> runtimeOptions,
        ISubjectResolver subjectResolver,
        ILogger<NatsLeaderElection> logger,
        string streamName)
    {
        _transport = transport;
        _runtimeOptions = runtimeOptions;
        _subjectResolver = subjectResolver;
        _options = options.Value;
        _logger = logger;
        _streamName = streamName ?? throw new ArgumentNullException(nameof(streamName));
        _leaderKey = $"leader.{_streamName}";
    }

    // ── Lazy Initialisation ───────────────────────────────────────────────────

    private async Task EnsureInitialisedAsync(CancellationToken ct = default)
    {
        if (_initialized) return;

        await _initLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_initialized) return;

            _logger.LogDebug("Lazy-initializing NATS KV store for leader election (stream '{StreamName}')", _streamName);

            _nats = GetNatsConnection();
            var kvContext = _nats.CreateKeyValueStoreContext();

            try
            {
                _kvStore = await kvContext.GetStoreAsync(
                    _subjectResolver.GetLeaderElectionBucketSubject(),
                    cancellationToken: ct).ConfigureAwait(false);

                _logger.LogDebug("Using existing KV bucket '{Bucket}'", _subjectResolver.GetLeaderElectionBucketSubject());
            }
            catch (NatsKVException)
            {
                _logger.LogInformation(
                    "Creating KV bucket '{Bucket}' with TTL {Ttl}",
                    _subjectResolver.GetLeaderElectionBucketSubject(),
                    _options.LeaderLockTtl);

                _kvStore = await kvContext.CreateStoreAsync(
                    new NatsKVConfig(_subjectResolver.GetLeaderElectionBucketSubject())
                    {
                        History = 1,
                        MaxAge = _options.LeaderLockTtl,
                        MaxBytes = 1024 * 1024,
                        Storage = NatsKVStorageType.Memory
                    }, ct).ConfigureAwait(false);
            }

            StartLeaderWatch();
            _initialized = true;

            _logger.LogDebug("NATS KV leader election initialized for stream '{StreamName}'", _streamName);
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

        if (_isLeader)
        {
            _logger.LogDebug(
                "TryAcquireLeadership called but already leader for stream '{StreamName}' — ignoring",
                _streamName);
            return true;
        }
        
        try
        {
            _logger.LogDebug(
                "Attempting to acquire leadership for stream '{StreamName}' (key '{Key}')",
                _streamName, _leaderKey);

            // CreateAsync is atomic: succeeds only if key does not exist.
            // If another instance holds the key (even with 1ms left on TTL), this throws.
            var revision = await _kvStore!.CreateAsync(
                _leaderKey,
                Encoding.UTF8.GetBytes(_runtimeOptions.Value.InstanceId),
                cancellationToken: ct).ConfigureAwait(false);

            // Store the revision — renewal MUST use this exact value to prove
            // we are renewing our own entry, not overwriting someone else's.
            _leaderRevision = revision;

            var newEpoch = Interlocked.Increment(ref _currentEpoch);
            _isLeader = true;

            _logger.LogInformation(
                "Instance {InstanceId} acquired leadership for stream '{StreamName}' " +
                "(epoch {Epoch}, revision {Revision})",
                _runtimeOptions.Value.InstanceId, _streamName, newEpoch, revision);

            StartRenewalLoop();
            OnLeadershipChanged?.Invoke(newEpoch);
            return true;
        }
        catch (NatsKVWrongLastRevisionException)
        {
            _logger.LogDebug(
                "Leadership already held for stream '{StreamName}' (revision conflict)",
                _streamName);
            return false;
        }
        catch (NatsKVCreateException)
        {
            _logger.LogDebug(
                "Leadership already held for stream '{StreamName}' (key exists)",
                _streamName);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error attempting to acquire leadership for stream '{StreamName}'", _streamName);
            return false;
        }
    }

    public async Task RenewLeadershipAsync(CancellationToken ct = default)
    {
        await EnsureInitialisedAsync(ct).ConfigureAwait(false);

        if (!_isLeader)
            throw new InvalidOperationException("Cannot renew: this instance is not the leader.");

        // UpdateAsync with the last known revision is the critical split-brain prevention.
        //
        // Scenario without this fix:
        //   T+0:   Instance A is leader (revision 5)
        //   T+500: A pauses (GC pause, network blip)
        //   T+501: A's TTL expires, key deleted
        //   T+502: Instance B wins election (revision 6)
        //   T+503: A resumes, PutAsync overwrites B's entry → SPLIT BRAIN
        //
        // With UpdateAsync(revision: 5):
        //   T+503: A calls UpdateAsync(revision=5), NATS rejects because current revision is 6
        //   T+503: A receives WrongLastRevision, correctly self-demotes
        //
        var newRevision = await _kvStore!.UpdateAsync(
            _leaderKey,
            Encoding.UTF8.GetBytes(_runtimeOptions.Value.InstanceId),
            _leaderRevision,  // Must match current server revision
            cancellationToken: ct).ConfigureAwait(false);

        // Update our stored revision for the next renewal call
        _leaderRevision = newRevision;

        _logger.LogTrace(
            "Renewed leadership for stream '{StreamName}' (instance {InstanceId}, epoch {Epoch}, revision {Revision})",
            _streamName, _runtimeOptions.Value.InstanceId, _currentEpoch, newRevision);
    }

    public async Task ReleaseLeadershipAsync(CancellationToken ct = default)
    {
        if (!_isLeader) return;

        try
        {
            if (_kvStore != null)
            {
                await _kvStore.DeleteAsync(_leaderKey, cancellationToken: ct).ConfigureAwait(false);

                _logger.LogInformation(
                    "Instance {InstanceId} released leadership for stream '{StreamName}' (epoch {Epoch})",
                    _runtimeOptions.Value.InstanceId, _streamName, _currentEpoch);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Error releasing leadership lock for stream '{StreamName}' (may have already expired)",
                _streamName);
        }
        finally
        {
            await HandleLeadershipLostAsync().ConfigureAwait(false);
        }
    }

    // ── Background Tasks ──────────────────────────────────────────────────────

    private void StartRenewalLoop()
    {
        _renewalCts?.Cancel();
        _renewalCts?.Dispose();
        _renewalCts = new CancellationTokenSource();
        var ct = _renewalCts.Token;

        var capturedRevision = _leaderRevision;

        _ = Task.Run(async () =>
        {
            _logger.LogDebug(
                "Renewal loop started for stream '{StreamName}' (revision '{Revision}') (interval {Interval}ms)",
                _streamName, capturedRevision, _options.LeaderHeartbeatInterval.TotalMilliseconds);

            var consecutiveFailures = 0;

            while (!ct.IsCancellationRequested && _isLeader)
            {
                try
                {
                    await Task.Delay(_options.LeaderHeartbeatInterval, ct).ConfigureAwait(false);
                    
                    if (ct.IsCancellationRequested) break; // ← check après le delay

                    await RenewLeadershipAsync(ct).ConfigureAwait(false);
                    consecutiveFailures = 0; // Reset on success
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (NatsKVWrongLastRevisionException)
                {
                    // Another instance has taken over — this is definitive, not transient.
                    // Self-demote immediately without waiting for more failures.
                    _logger.LogWarning(
                        "Revision conflict on renewal for stream '{StreamName}' — " +
                        "another instance has taken leadership. Self-demoting.",
                        _streamName);
                    await HandleLeadershipLostAsync().ConfigureAwait(false);
                    break;
                }
                catch (Exception ex)
                {
                    consecutiveFailures++;
                    _logger.LogError(ex,
                        "Renewal failure #{Count} for stream '{StreamName}'",
                        consecutiveFailures, _streamName);

                    // Only self-demote after sustained failures, not on a single transient error.
                    // This avoids unnecessary leader churn from momentary NATS hiccups.
                    if (consecutiveFailures >= MaxConsecutiveRenewalFailures)
                    {
                        _logger.LogWarning(
                            "Exceeded {Max} consecutive renewal failures for stream '{StreamName}'. " +
                            "Self-demoting to prevent stale leadership.",
                            MaxConsecutiveRenewalFailures, _streamName);
                        await HandleLeadershipLostAsync().ConfigureAwait(false);
                        break;
                    }
                }
            }

            _logger.LogDebug("Renewal loop stopped for stream '{StreamName}'", _streamName);
        }, ct);
    }

    private void StartLeaderWatch()
    {
        _watchCts?.Cancel();
        _watchCts = new CancellationTokenSource();
        var ct = _watchCts.Token;

        _ = Task.Run(async () =>
        {
            _logger.LogDebug(
                "Leader watch started for stream '{StreamName}' (key '{Key}')",
                _streamName, _leaderKey);

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await foreach (var entry in _kvStore!
                                       .WatchAsync<byte[]>(_leaderKey, cancellationToken: ct)
                                       .ConfigureAwait(false))
                    {
                        if (entry.Operation is NatsKVOperation.Del or NatsKVOperation.Purge)
                        {
                            _logger.LogInformation(
                                "Leader key deleted for stream '{StreamName}' (op={Op}), entering election",
                                _streamName, entry.Operation);

                            if (!_isLeader)
                            {
                                // Jitter prevents all followers from stampeding NATS simultaneously.
                                // The window is small (0-50ms) — well within our failover budget.
                                var jitterMs = Random.Shared.Next(0, 50);
                                if (jitterMs > 0)
                                    await Task.Delay(jitterMs, ct).ConfigureAwait(false);

                                await TryAcquireLeadershipAsync(ct).ConfigureAwait(false);
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (NatsKVKeyNotFoundException)
                {
                    // Key doesn't exist yet — this is normal on startup before any leader has
                    // been elected. Wait briefly and retry the watch.
                    _logger.LogDebug(
                        "Leader key '{Key}' not found for stream '{StreamName}', retrying watch in 500ms",
                        _leaderKey, _streamName);
                    await Task.Delay(500, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Leader watch faulted for stream '{StreamName}', restarting in 1s",
                        _streamName);
                    await Task.Delay(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
                }
            }

            _logger.LogDebug("Leader watch stopped for stream '{StreamName}'", _streamName);
        }, ct);
    }

    private Task HandleLeadershipLostAsync()
    {
        if (!_isLeader) return Task.CompletedTask;

        _isLeader = false;
        _leaderRevision = 0; // Reset so we don't accidentally use a stale revision
        _renewalCts?.Cancel();

        var epoch = _currentEpoch;
        _logger.LogWarning(
            "Instance {InstanceId} lost leadership for stream '{StreamName}' (epoch {Epoch})",
            _runtimeOptions.Value.InstanceId, _streamName, epoch);

        OnLeadershipChanged?.Invoke(epoch);
        return Task.CompletedTask;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private NatsConnection GetNatsConnection()
    {
        if (_transport is INatsConnectionAccessor accessor)
            return accessor.NatsConnection;

        var field = typeof(NatsConnectionManager)
            .GetField("_connection",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        var conn = field?.GetValue(_transport) as NatsConnection;
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

        _logger.LogDebug("NatsLeaderElection disposed for stream '{StreamName}'", _streamName);
        return ValueTask.CompletedTask;
    }
}