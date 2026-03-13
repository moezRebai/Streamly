using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Runtime.Configuration;

namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Monitors leader heartbeats and attempts to acquire leadership if leader dies
/// </summary>
internal class LeaderMonitor(
    ILeaderElectionService leaderElection,
    IOptions<LeaderElectionOptions> options,
    ISubjectResolver subjects,
    IStreamingTransport transport,
    ILogger<LeaderMonitor> logger)
    : IAsyncDisposable
{
    private readonly ILeaderElectionService _leaderElection = leaderElection ?? throw new ArgumentNullException(nameof(leaderElection));
    private readonly LeaderElectionOptions _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
    private readonly ILogger<LeaderMonitor> _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    
    private CancellationTokenSource? _runningCts;
    private Task? _monitorTask;
    private bool _disposed;

    // Track when we last received a heartbeat (updated by LeaderElectionService)
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

        // ✅ Souscrire aux heartbeats NATS Core
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

    public async Task StopAsync(CancellationToken cancellationToken = default)
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

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
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
        catch (OperationCanceledException) { }
    }

    private async Task CheckLeaderHealthAsync(CancellationToken cancellationToken)
    {
        var timeSinceLastHeartbeat = DateTime.UtcNow - _lastHeartbeatReceived;

        // Check if leader appears dead (no heartbeat for DeadThreshold duration)
        // _lastHeartbeatReceived est initialisé à UtcNow dans StartAsync,
        // donc cette condition ne se déclenche qu'après DeadThreshold réel.
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