using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Runtime.Configuration;

namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Monitors leader heartbeats and attempts to acquire leadership if leader dies
/// Internal component - managed by StreamLeadershipCoordinator
/// 
/// MIGRATION NOTE: No changes needed! LeaderMonitor doesn't use Redis directly.
///                 It monitors LeaderElectionService events and the Infrastructure
///                 layer handles heartbeat subscription internally.
/// </summary>
internal class LeaderMonitor : IAsyncDisposable
{
    private readonly ILeaderElectionService _leaderElection;
    private readonly LeaderElectionOptions _options;
    private readonly ILogger<LeaderMonitor> _logger;
    
    private CancellationTokenSource? _runningCts;
    private Task? _monitorTask;
    private bool _disposed;

    // Track when we last received a heartbeat (updated by LeaderElectionService)
    private DateTime _lastHeartbeatReceived = DateTime.MinValue;

    public LeaderMonitor(
        ILeaderElectionService leaderElection,
        IOptions<LeaderElectionOptions> options,
        ILogger<LeaderMonitor> logger)
    {
        _leaderElection = leaderElection ?? throw new ArgumentNullException(nameof(leaderElection));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Subscribe to leadership changes to track heartbeat received time
        _leaderElection.LeadershipChanged += OnLeadershipChanged;
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

        if (_runningCts != null)
        {
            _logger.LogWarning(
                "LeaderMonitor already started for stream '{StreamName}'",
                _leaderElection.StreamName);
            return;
        }

        _logger.LogInformation(
            "Starting leader monitor for stream '{StreamName}'",
            _leaderElection.StreamName);

        _runningCts = new CancellationTokenSource();
        _monitorTask = RunMonitorLoopAsync(_runningCts.Token);
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_runningCts == null)
            return;

        _logger.LogInformation(
            "Stopping leader monitor for stream '{StreamName}'",
            _leaderElection.StreamName);

        _runningCts.Cancel();

        if (_monitorTask != null)
        {
            try
            {
                await _monitorTask;
            }
            catch (OperationCanceledException)
            {
                // Expected when stopping
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Error stopping leader monitor for stream '{StreamName}'",
                    _leaderElection.StreamName);
            }
        }

        _runningCts?.Dispose();
        _runningCts = null;
        _monitorTask = null;
    }

    private async Task RunMonitorLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug(
            "Leader monitor loop started for stream '{StreamName}'",
            _leaderElection.StreamName);

        // Check every 100ms (configurable, but typically much faster than heartbeat interval)
        var checkInterval = TimeSpan.FromMilliseconds(100);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Only monitor if we're a follower
                if (!_leaderElection.IsLeader)
                {
                    try
                    {
                        await CheckLeaderHealthAsync(cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex,
                            "Error checking leader health for stream '{StreamName}'",
                            _leaderElection.StreamName);
                    }
                }

                await Task.Delay(checkInterval, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug(
                "Leader monitor loop cancelled for stream '{StreamName}'",
                _leaderElection.StreamName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Unexpected error in leader monitor loop for stream '{StreamName}'",
                _leaderElection.StreamName);
        }
    }

    private async Task CheckLeaderHealthAsync(CancellationToken cancellationToken)
    {
        var timeSinceLastHeartbeat = DateTime.UtcNow - _lastHeartbeatReceived;

        // If we've never received a heartbeat, try to acquire immediately
        if (_lastHeartbeatReceived == DateTime.MinValue)
        {
            _logger.LogDebug(
                "No heartbeat received yet for stream '{StreamName}', attempting to acquire leadership",
                _leaderElection.StreamName);

            await _leaderElection.TryAcquireLeadershipAsync(cancellationToken);
            return;
        }

        // Check if leader appears dead (no heartbeat for DeadThreshold duration)
        if (timeSinceLastHeartbeat > _options.DeadThreshold)
        {
            _logger.LogWarning(
                "Leader appears dead for stream '{StreamName}' (no heartbeat for {Duration}ms), attempting to acquire leadership",
                _leaderElection.StreamName,
                timeSinceLastHeartbeat.TotalMilliseconds);

            await _leaderElection.TryAcquireLeadershipAsync(cancellationToken);
        }
        else
        {
            _logger.LogTrace(
                "Leader healthy for stream '{StreamName}', last heartbeat {Duration}ms ago",
                _leaderElection.StreamName,
                timeSinceLastHeartbeat.TotalMilliseconds);
        }
    }

    private void OnLeadershipChanged(object? sender, LeadershipChangedEventArgs e)
    {
        // When we receive notification of any leadership change, update heartbeat time
        // This prevents false positives when we first start or when leadership changes
        _lastHeartbeatReceived = DateTime.UtcNow;

        _logger.LogDebug(
            "Leadership changed for stream '{StreamName}', resetting heartbeat timer",
            _leaderElection.StreamName);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        
        _leaderElection.LeadershipChanged -= OnLeadershipChanged;
        
        await StopAsync();
    }
}