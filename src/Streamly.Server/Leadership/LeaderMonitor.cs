using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;

namespace Streamly.Server.Leadership;

/// <summary>
/// Monitors leader heartbeats and attempts to acquire leadership if leader dies.
/// </summary>
internal class LeaderMonitor(
    ILeaderElectionService leaderElection,
    IOptions<StreamlySettings> options,
    ISubjectResolver subjects,
    IStreamingTransport transport,
    ILogger<LeaderMonitor> logger)
    : IAsyncDisposable
{
    private readonly ILeaderElectionService _leaderElection = leaderElection ?? throw new ArgumentNullException(nameof(leaderElection));
    private readonly StreamlySettings.LeaderElectionSection _options = options?.Value.LeaderElection ?? throw new ArgumentNullException(nameof(options));
    private readonly ILogger<LeaderMonitor> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private CancellationTokenSource? _runningCts;
    private Task? _monitorTask;
    private bool _disposed;

    private DateTime _lastHeartbeatReceived = DateTime.MinValue;

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

        _lastHeartbeatReceived = DateTime.UtcNow;

        var heartbeatSubject = subjects.GetHeartbeatSubject(_leaderElection.StreamName);
        await transport.SubscribeAsync(
            heartbeatSubject,
            OnHeartbeatReceivedAsync,
            cancellationToken);

        _logger.LogInformation(
            "Starting leader monitor for stream '{StreamName}'",
            _leaderElection.StreamName);

        _runningCts = new CancellationTokenSource();
        _monitorTask = RunMonitorLoopAsync(_runningCts.Token);
    }

    public async Task StopAsync()
    {
        if (_runningCts == null)
            return;

        _logger.LogInformation(
            "Stopping leader monitor for stream '{StreamName}'",
            _leaderElection.StreamName);

        await _runningCts.CancelAsync();

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
        var checkInterval = TimeSpan.FromMilliseconds(100);
        var wasLeader     = _leaderElection.IsLeader;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var isLeader = _leaderElection.IsLeader;

                // When this instance just lost leadership, reset the heartbeat clock.
                // While we were leader the monitor loop was idle, so _lastHeartbeatReceived
                // was never updated — without this reset the monitor would immediately fire
                // on a stale timestamp and race to re-acquire before the new leader has
                // published its first heartbeat.
                if (wasLeader && !isLeader)
                {
                    _lastHeartbeatReceived = DateTime.UtcNow;
                    _logger.LogDebug(
                        "LeaderMonitor reset heartbeat clock after leadership loss for stream '{StreamName}'",
                        _leaderElection.StreamName);
                }

                wasLeader = isLeader;

                if (!isLeader)
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
        catch (OperationCanceledException) { }
    }

    private bool _leaderDeadLogged;

    private async Task CheckLeaderHealthAsync(CancellationToken cancellationToken)
    {
        var timeSinceLastHeartbeat = DateTime.UtcNow - _lastHeartbeatReceived;

        if (timeSinceLastHeartbeat.TotalMilliseconds <= _options.DeadThresholdMs)
        {
            _leaderDeadLogged = false; // leader is alive again, reset flag
            _logger.LogTrace(
                "Leader healthy for stream '{StreamName}', last heartbeat {Duration}ms ago",
                _leaderElection.StreamName,
                timeSinceLastHeartbeat.TotalMilliseconds);
            return;
        }

        // Log the warning once when leader first appears dead, not on every retry
        if (!_leaderDeadLogged)
        {
            _logger.LogWarning(
                "Leader appears dead for stream '{StreamName}' (no heartbeat for {Duration}ms), attempting to acquire leadership",
                _leaderElection.StreamName,
                timeSinceLastHeartbeat.TotalMilliseconds);
            _leaderDeadLogged = true;
        }

        var acquired = await _leaderElection.TryAcquireLeadershipAsync(cancellationToken);

        if (!acquired)
        {
            // Key still held by dying leader (TTL not expired yet) — back off to avoid
            // hammering NATS with CreateAsync calls every 100ms
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
        }
    }

    private Task OnHeartbeatReceivedAsync(byte[] data)
    {
        _lastHeartbeatReceived = DateTime.UtcNow;

        _logger.LogTrace("Heartbeat received for stream '{StreamName}'",
            _leaderElection.StreamName);

        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        await StopAsync();
    }
}
