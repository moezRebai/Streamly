using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Streamly.Core.Runtime.Channel;
using Streamly.Core.Runtime.Configuration;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Manages leader election for a specific stream using Redis-based distributed locking
/// Each stream has independent leader election
/// </summary>
public class LeaderElectionService : ILeaderElectionService
{
    private readonly string _streamName;
    private readonly string _instanceId;
    private readonly IRedisConnectionManager _redis;
    private readonly IMessageSerializer _serializer;
    private readonly IChannelNameResolver _channelResolver;
    private readonly LeaderElectionOptions _options;
    private readonly ILogger<LeaderElectionService> _logger;
    
    private readonly SemaphoreSlim _stateLock = new(1, 1);
    private readonly string _leaderLockKey;
    private readonly string _heartbeatChannel;
    
    private LeadershipState _state = LeadershipState.Follower;
    private long _currentEpoch;
    private string? _currentLeaderId;
    private DateTime _lastHeartbeatReceived = DateTime.MinValue;
    private CancellationTokenSource? _heartbeatSubscriptionCts;
    private bool _disposed;

    public event EventHandler<LeadershipChangedEventArgs>? LeadershipChanged;

    public string StreamName => _streamName;
    public string InstanceId => _instanceId;
    public LeadershipState State => _state;
    public bool IsLeader => _state == LeadershipState.Leader;
    public long CurrentEpoch => _currentEpoch;
    public string? CurrentLeaderId => _currentLeaderId;

    public LeaderElectionService(
        string streamName,
        string instanceId,
        IRedisConnectionManager redis,
        IMessageSerializer serializer,
        IChannelNameResolver channelResolver,
        IOptions<LeaderElectionOptions> options,
        ILogger<LeaderElectionService> logger)
    {
        _streamName = streamName ?? throw new ArgumentNullException(nameof(streamName));
        _instanceId = instanceId ?? throw new ArgumentNullException(nameof(instanceId));
        _redis = redis ?? throw new ArgumentNullException(nameof(redis));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _channelResolver = channelResolver ?? throw new ArgumentNullException(nameof(channelResolver));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _leaderLockKey = _channelResolver.GetLeaderLockKey(_streamName);
        _heartbeatChannel = _channelResolver.GetHeartbeatChannel(_streamName);
        
        _logger.LogInformation(
            "LeaderElectionService created for stream '{StreamName}' with instance '{InstanceId}'",
            _streamName,
            _instanceId);
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Starting leader election service for stream '{StreamName}'",
            _streamName);

        // Subscribe to heartbeat channel to monitor leader
        _heartbeatSubscriptionCts = new CancellationTokenSource();
        await SubscribeToHeartbeatAsync(_heartbeatSubscriptionCts.Token);
        
        // Try to acquire leadership immediately
        await TryAcquireLeadershipAsync(cancellationToken);
        
        _logger.LogInformation(
            "Leader election service started for stream '{StreamName}', current state: {State}",
            _streamName,
            _state);
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Stopping leader election service for stream '{StreamName}'",
            _streamName);

        // Cancel heartbeat subscription
        _heartbeatSubscriptionCts?.Cancel();
        _heartbeatSubscriptionCts?.Dispose();
        _heartbeatSubscriptionCts = null;

        // Release leadership if we're the leader
        if (_state == LeadershipState.Leader)
        {
            await ReleaseLeadershipAsync(cancellationToken);
        }
        
        _logger.LogInformation(
            "Leader election service stopped for stream '{StreamName}'",
            _streamName);
    }

    public async Task<bool> TryAcquireLeadershipAsync(CancellationToken cancellationToken = default)
    {
        await _stateLock.WaitAsync(cancellationToken);
        try
        {
            // Already leader, nothing to do
            if (_state == LeadershipState.Leader)
            {
                _logger.LogDebug(
                    "Already leader for stream '{StreamName}', skipping acquisition",
                    _streamName);
                return true;
            }

            _logger.LogInformation(
                "Attempting to acquire leadership for stream '{StreamName}'",
                _streamName);

            // Transition to candidate state
            await TransitionToStateAsync(LeadershipState.Candidate);

            // Read current epoch from Redis (if lock exists)
            var currentEpochFromRedis = await GetCurrentEpochFromRedisAsync(cancellationToken);
            var newEpoch = currentEpochFromRedis + 1;

            _logger.LogDebug(
                "Current epoch from Redis: {CurrentEpoch}, attempting acquisition with epoch: {NewEpoch}",
                currentEpochFromRedis,
                newEpoch);

            // Create lock value with new epoch
            var lockValue = new LeaderLockValue
            {
                InstanceId = _instanceId,
                Epoch = newEpoch,
                AcquiredAt = DateTime.UtcNow,
                StreamName = _streamName
            };

            var lockValueJson = JsonSerializer.Serialize(lockValue);

            // Try to acquire lock using SET NX EX (atomic operation)
            var db = _redis.Multiplexer.GetDatabase();
            
            var acquired = await db.StringSetAsync(
                _leaderLockKey,
                lockValueJson,
                _options.LockTtl,
                When.NotExists);

            if (acquired)
            {
                // Successfully acquired leadership
                _currentEpoch = newEpoch;
                _currentLeaderId = _instanceId;
                await TransitionToStateAsync(LeadershipState.Leader);
                
                _logger.LogInformation(
                    "Successfully acquired leadership for stream '{StreamName}' with epoch {Epoch}",
                    _streamName,
                    _currentEpoch);
                
                return true;
            }
            else
            {
                // Failed to acquire, someone else is leader
                _logger.LogDebug(
                    "Failed to acquire leadership for stream '{StreamName}', lock already held",
                    _streamName);
                
                // Read who the current leader is
                await ReadCurrentLeaderFromRedisAsync(cancellationToken);
                
                // Transition back to follower
                await TransitionToStateAsync(LeadershipState.Follower);
                
                return false;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error attempting to acquire leadership for stream '{StreamName}'",
                _streamName);
            
            // On error, transition back to follower
            await TransitionToStateAsync(LeadershipState.Follower);
            
            return false;
        }
        finally
        {
            _stateLock.Release();
        }
    }

    public async Task<bool> RenewLeadershipAsync(CancellationToken cancellationToken = default)
    {
        await _stateLock.WaitAsync(cancellationToken);
        try
        {
            if (_state != LeadershipState.Leader)
            {
                _logger.LogWarning(
                    "Cannot renew leadership for stream '{StreamName}' - not currently leader (state: {State})",
                    _streamName,
                    _state);
                return false;
            }

            _logger.LogDebug(
                "Renewing leadership for stream '{StreamName}' with epoch {Epoch}",
                _streamName,
                _currentEpoch);

            // Read current lock from Redis
            var db = _redis.Multiplexer.GetDatabase();
            
            var currentLockJson = await db.StringGetAsync(_leaderLockKey);

            if (currentLockJson.IsNullOrEmpty)
            {
                // Lock expired or was deleted
                _logger.LogWarning(
                    "Leadership lock expired for stream '{StreamName}', lost leadership",
                    _streamName);
                
                await TransitionToStateAsync(LeadershipState.Follower);
                return false;
            }

            var currentLock = JsonSerializer.Deserialize<LeaderLockValue>(currentLockJson.ToString());
            
            if (currentLock == null)
            {
                _logger.LogError(
                    "Failed to deserialize leader lock for stream '{StreamName}'",
                    _streamName);
                
                await TransitionToStateAsync(LeadershipState.Follower);
                return false;
            }

            // Verify it's still our lock
            if (currentLock.InstanceId != _instanceId || currentLock.Epoch != _currentEpoch)
            {
                _logger.LogWarning(
                    "Leadership taken by another instance for stream '{StreamName}'. " +
                    "Expected: (Instance={OurInstance}, Epoch={OurEpoch}), " +
                    "Actual: (Instance={ActualInstance}, Epoch={ActualEpoch})",
                    _streamName,
                    _instanceId,
                    _currentEpoch,
                    currentLock.InstanceId,
                    currentLock.Epoch);
                
                _currentLeaderId = currentLock.InstanceId;
                _currentEpoch = currentLock.Epoch;
                await TransitionToStateAsync(LeadershipState.Follower);
                
                return false;
            }

            // Renew the lock with same value (extend TTL)
            var renewed = await db.StringSetAsync(
                _leaderLockKey,
                currentLockJson,
                _options.LockTtl);

            if (renewed)
            {
                _logger.LogTrace(
                    "Successfully renewed leadership for stream '{StreamName}'",
                    _streamName);
                return true;
            }
            else
            {
                _logger.LogWarning(
                    "Failed to renew leadership lock for stream '{StreamName}'",
                    _streamName);
                
                await TransitionToStateAsync(LeadershipState.Follower);
                return false;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error renewing leadership for stream '{StreamName}'",
                _streamName);
            
            await TransitionToStateAsync(LeadershipState.Follower);
            return false;
        }
        finally
        {
            _stateLock.Release();
        }
    }

    public async Task ReleaseLeadershipAsync(CancellationToken cancellationToken = default)
    {
        await _stateLock.WaitAsync(cancellationToken);
        try
        {
            if (_state != LeadershipState.Leader)
            {
                _logger.LogDebug(
                    "Not leader for stream '{StreamName}', nothing to release",
                    _streamName);
                return;
            }

            _logger.LogInformation(
                "Releasing leadership for stream '{StreamName}'",
                _streamName);

            // Delete the lock key from Redis
            var db = _redis.Multiplexer.GetDatabase();
            
            // Only delete if it's still our lock (verify epoch)
            var script = @"
                local lockValue = redis.call('GET', KEYS[1])
                if lockValue then
                    local lock = cjson.decode(lockValue)
                    if lock.instanceId == ARGV[1] and lock.epoch == tonumber(ARGV[2]) then
                        return redis.call('DEL', KEYS[1])
                    end
                end
                return 0";

            var deleted = await db.ScriptEvaluateAsync(
                script,
                new RedisKey[] { _leaderLockKey },
                new RedisValue[] { _instanceId, _currentEpoch });

            if ((int)deleted == 1)
            {
                _logger.LogInformation(
                    "Successfully released leadership for stream '{StreamName}'",
                    _streamName);
            }
            else
            {
                _logger.LogWarning(
                    "Leadership already released or taken by another instance for stream '{StreamName}'",
                    _streamName);
            }

            await TransitionToStateAsync(LeadershipState.Follower);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error releasing leadership for stream '{StreamName}'",
                _streamName);
            
            // Still transition to follower even on error
            await TransitionToStateAsync(LeadershipState.Follower);
        }
        finally
        {
            _stateLock.Release();
        }
    }

    private async Task SubscribeToHeartbeatAsync(CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogDebug(
                "Subscribing to heartbeat channel for stream '{StreamName}'",
                _streamName);

            await _redis.SubscribeAsync(_heartbeatChannel, async (data) =>
            {
                try
                {
                    var heartbeat = _serializer.Deserialize<HeartbeatMessage>(data);
                    await OnHeartbeatReceivedAsync(heartbeat);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Error processing heartbeat for stream '{StreamName}'",
                        _streamName);
                }
            });
            
            _logger.LogInformation(
                "Subscribed to heartbeat channel for stream '{StreamName}'",
                _streamName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to subscribe to heartbeat channel for stream '{StreamName}'",
                _streamName);
            throw;
        }
    }

    private async Task OnHeartbeatReceivedAsync(HeartbeatMessage heartbeat)
    {
        if (heartbeat.StreamName != _streamName)
        {
            _logger.LogWarning(
                "Received heartbeat for wrong stream. Expected: '{ExpectedStream}', Actual: '{ActualStream}'",
                _streamName,
                heartbeat.StreamName);
            return;
        }

        _logger.LogTrace(
            "Received heartbeat for stream '{StreamName}' from leader '{LeaderId}' with epoch {Epoch}",
            _streamName,
            heartbeat.LeaderId,
            heartbeat.Epoch);

        await _stateLock.WaitAsync();
        try
        {
            // Update last heartbeat received time
            _lastHeartbeatReceived = DateTime.UtcNow;

            // Update epoch if higher (new leader elected)
            if (heartbeat.Epoch > _currentEpoch)
            {
                _logger.LogInformation(
                    "Epoch increased for stream '{StreamName}': {OldEpoch} → {NewEpoch}, new leader: '{LeaderId}'",
                    _streamName,
                    _currentEpoch,
                    heartbeat.Epoch,
                    heartbeat.LeaderId);
                
                _currentEpoch = heartbeat.Epoch;
                _currentLeaderId = heartbeat.LeaderId;
                
                // If we thought we were leader but epoch changed, step down
                if (_state == LeadershipState.Leader && heartbeat.LeaderId != _instanceId)
                {
                    _logger.LogWarning(
                        "Detected new leader with higher epoch for stream '{StreamName}', stepping down",
                        _streamName);
                    
                    await TransitionToStateAsync(LeadershipState.Follower);
                }
            }
            else if (heartbeat.Epoch == _currentEpoch)
            {
                // Same epoch, update leader ID
                _currentLeaderId = heartbeat.LeaderId;
            }
            else
            {
                // Received heartbeat with lower epoch (stale message)
                _logger.LogDebug(
                    "Received stale heartbeat for stream '{StreamName}' with epoch {HeartbeatEpoch} < current {CurrentEpoch}",
                    _streamName,
                    heartbeat.Epoch,
                    _currentEpoch);
            }
        }
        finally
        {
            _stateLock.Release();
        }
    }

    private async Task<long> GetCurrentEpochFromRedisAsync(CancellationToken cancellationToken)
    {
        try
        {
            var db = _redis.Multiplexer.GetDatabase();
            
            var lockValueJson = await db.StringGetAsync(_leaderLockKey);
            
            if (lockValueJson.IsNullOrEmpty)
            {
                _logger.LogDebug(
                    "No existing leader lock found for stream '{StreamName}', starting with epoch 0",
                    _streamName);
                return 0;
            }

            var lockValue = JsonSerializer.Deserialize<LeaderLockValue>(lockValueJson.ToString());
            
            if (lockValue == null)
            {
                _logger.LogWarning(
                    "Failed to deserialize leader lock for stream '{StreamName}', assuming epoch 0",
                    _streamName);
                return 0;
            }

            _logger.LogDebug(
                "Current epoch from Redis for stream '{StreamName}': {Epoch} (held by '{InstanceId}')",
                _streamName,
                lockValue.Epoch,
                lockValue.InstanceId);

            return lockValue.Epoch;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error reading current epoch from Redis for stream '{StreamName}'",
                _streamName);
            return 0;
        }
    }

    private async Task ReadCurrentLeaderFromRedisAsync(CancellationToken cancellationToken)
    {
        try
        {
            var db = _redis.Multiplexer.GetDatabase();
            
            var lockValueJson = await db.StringGetAsync(_leaderLockKey);
            
            if (lockValueJson.IsNullOrEmpty)
            {
                _currentLeaderId = null;
                return;
            }

            var lockValue = JsonSerializer.Deserialize<LeaderLockValue>(lockValueJson.ToString());
            
            if (lockValue != null)
            {
                _currentLeaderId = lockValue.InstanceId;
                _currentEpoch = lockValue.Epoch;
                
                _logger.LogDebug(
                    "Current leader for stream '{StreamName}': '{LeaderId}' with epoch {Epoch}",
                    _streamName,
                    _currentLeaderId,
                    _currentEpoch);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error reading current leader from Redis for stream '{StreamName}'",
                _streamName);
        }
    }

    private async Task TransitionToStateAsync(LeadershipState newState)
    {
        if (_state == newState)
            return;

        var previousState = _state;
        _state = newState;

        _logger.LogInformation(
            "Leadership state transition for stream '{StreamName}': {PreviousState} → {NewState} (Epoch: {Epoch})",
            _streamName,
            previousState,
            newState,
            _currentEpoch);

        // Raise event
        try
        {
            var eventArgs = new LeadershipChangedEventArgs(
                previousState,
                newState,
                _streamName,
                _currentEpoch,
                _currentLeaderId);

            LeadershipChanged?.Invoke(this, eventArgs);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error raising LeadershipChanged event for stream '{StreamName}'",
                _streamName);
        }

        await Task.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        _heartbeatSubscriptionCts?.Cancel();
        _heartbeatSubscriptionCts?.Dispose();
        _stateLock.Dispose();

        _logger.LogDebug(
            "LeaderElectionService disposed for stream '{StreamName}'",
            _streamName);
    }
}
