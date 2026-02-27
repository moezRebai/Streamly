using Microsoft.Extensions.Logging;
using Streamly.Core.Abstractions;

namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Runtime wrapper around Infrastructure's ILeaderElection.
/// Adapts Infrastructure events to Runtime-specific LeadershipChanged events.
/// 
/// MIGRATION NOTE: This was previously a 600-line Redis-specific implementation.
/// Now it's a 100-line adapter that delegates to Infrastructure layer (Redis or NATS).
/// </summary>
public class LeaderElectionService : ILeaderElectionService
{
    private readonly ILeaderElection _infrastructureLeaderElection;
    private readonly string _streamName;
    private readonly string _instanceId;
    private readonly ILogger<LeaderElectionService> _logger;
    
    private LeadershipState _state = LeadershipState.Follower;
    private long _currentEpoch;
    private string? _currentLeaderId;
    private bool _disposed;

    public event EventHandler<LeadershipChangedEventArgs>? LeadershipChanged;

    public string StreamName => _streamName;
    public string InstanceId => _instanceId;
    public LeadershipState State => _state;
    public bool IsLeader => _infrastructureLeaderElection.IsLeader;
    public long CurrentEpoch => _infrastructureLeaderElection.CurrentEpoch;
    public string? CurrentLeaderId => _currentLeaderId;

    public LeaderElectionService(
        string streamName,
        string instanceId,
        ILeaderElection infrastructureLeaderElection,
        ILogger<LeaderElectionService> logger)
    {
        _streamName = streamName ?? throw new ArgumentNullException(nameof(streamName));
        _instanceId = instanceId ?? throw new ArgumentNullException(nameof(instanceId));
        _infrastructureLeaderElection = infrastructureLeaderElection ?? throw new ArgumentNullException(nameof(infrastructureLeaderElection));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Subscribe to Infrastructure's leadership changes
        _infrastructureLeaderElection.OnLeadershipChanged += OnInfrastructureLeadershipChanged;
        
        _logger.LogInformation(
            "LeaderElectionService created for stream '{StreamName}' with instance '{InstanceId}'",
            _streamName,
            _instanceId);
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Starting leader election service for stream '{StreamName}'",
            _streamName);

        // Try to acquire leadership immediately
        // Infrastructure layer handles heartbeat subscription internally
        _ = TryAcquireLeadershipAsync(cancellationToken);
        
        _logger.LogInformation(
            "Leader election service started for stream '{StreamName}', current state: {State}",
            _streamName,
            _state);
        
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Stopping leader election service for stream '{StreamName}'",
            _streamName);

        // Release leadership if we're the leader
        if (IsLeader)
        {
            await ReleaseLeadershipAsync();
        }
        
        _logger.LogInformation(
            "Leader election service stopped for stream '{StreamName}'",
            _streamName);
    }

    public async Task<bool> TryAcquireLeadershipAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation(
                "Attempting to acquire leadership for stream '{StreamName}'",
                _streamName);

            var acquired = await _infrastructureLeaderElection.TryAcquireLeadershipAsync(cancellationToken);

            if (acquired)
            {
                _logger.LogInformation(
                    "Successfully acquired leadership for stream '{StreamName}' with epoch {Epoch}",
                    _streamName,
                    CurrentEpoch);
            }
            else
            {
                _logger.LogDebug(
                    "Failed to acquire leadership for stream '{StreamName}', lock already held",
                    _streamName);
            }

            return acquired;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error attempting to acquire leadership for stream '{StreamName}'",
                _streamName);
            return false;
        }
    }

    public async Task<bool> RenewLeadershipAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            if (!IsLeader)
            {
                _logger.LogWarning(
                    "Cannot renew leadership for stream '{StreamName}' - not currently leader",
                    _streamName);
                return false;
            }

            await _infrastructureLeaderElection.RenewLeadershipAsync(cancellationToken);
            
            _logger.LogTrace(
                "Renewed leadership for stream '{StreamName}' with epoch {Epoch}",
                _streamName,
                CurrentEpoch);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error renewing leadership for stream '{StreamName}'",
                _streamName);
            return false;
        }
    }

    public async Task ReleaseLeadershipAsync(CancellationToken cancellationToken = default)
    {
        if (!IsLeader)
        {
            _logger.LogDebug(
                "Not leader for stream '{StreamName}', nothing to release",
                _streamName);
            return;
        }

        try
        {
            _logger.LogInformation(
                "Releasing leadership for stream '{StreamName}'",
                _streamName);

            await _infrastructureLeaderElection.ReleaseLeadershipAsync(cancellationToken);

            _logger.LogInformation(
                "Successfully released leadership for stream '{StreamName}'",
                _streamName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error releasing leadership for stream '{StreamName}'",
                _streamName);
            throw;
        }
    }

    private void OnInfrastructureLeadershipChanged(int newEpoch)
    {
        var previousState = _state;
        var newState = IsLeader ? LeadershipState.Leader : LeadershipState.Follower;
        
        if (previousState == newState && _currentEpoch == newEpoch)
            return; // No actual change

        _state = newState;
        _currentEpoch = newEpoch;
        _currentLeaderId = IsLeader ? _instanceId : null;

        _logger.LogInformation(
            "Leadership state transition for stream '{StreamName}': {PreviousState} to {NewState} (Epoch: {Epoch})",
            _streamName,
            previousState,
            newState,
            _currentEpoch);

        // Raise Runtime-level event
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
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        _infrastructureLeaderElection.OnLeadershipChanged -= OnInfrastructureLeadershipChanged;

        _logger.LogDebug(
            "LeaderElectionService disposed for stream '{StreamName}'",
            _streamName);
    }
}