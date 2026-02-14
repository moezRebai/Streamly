using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Runtime.Channel;
using Streamly.Core.Runtime.Configuration;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Publishes heartbeat messages when instance is leader
/// Internal component - managed by StreamLeadershipCoordinator
/// </summary>
internal class HeartbeatPublisher : IAsyncDisposable
{
    private readonly ILeaderElectionService _leaderElection;
    private readonly IRedisConnectionManager _redis;
    private readonly IMessageSerializer _serializer;
    private readonly IChannelNameResolver _channelResolver;
    private readonly LeaderElectionOptions _options;
    private readonly ILogger<HeartbeatPublisher> _logger;
    
    private readonly string _heartbeatChannel;
    private CancellationTokenSource? _runningCts;
    private Task? _publishTask;
    private bool _disposed;

    public HeartbeatPublisher(
        ILeaderElectionService leaderElection,
        IRedisConnectionManager redis,
        IMessageSerializer serializer,
        IChannelNameResolver channelResolver,
        IOptions<LeaderElectionOptions> options,
        ILogger<HeartbeatPublisher> logger)
    {
        _leaderElection = leaderElection ?? throw new ArgumentNullException(nameof(leaderElection));
        _redis = redis ?? throw new ArgumentNullException(nameof(redis));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _channelResolver = channelResolver ?? throw new ArgumentNullException(nameof(channelResolver));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _heartbeatChannel = _channelResolver.GetHeartbeatChannel(_leaderElection.StreamName);
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_runningCts != null)
        {
            _logger.LogWarning(
                "HeartbeatPublisher already started for stream '{StreamName}'",
                _leaderElection.StreamName);
            return Task.CompletedTask;
        }

        _logger.LogInformation(
            "Starting heartbeat publisher for stream '{StreamName}'",
            _leaderElection.StreamName);

        _runningCts = new CancellationTokenSource();
        _publishTask = RunHeartbeatLoopAsync(_runningCts.Token);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_runningCts == null)
            return;

        _logger.LogInformation(
            "Stopping heartbeat publisher for stream '{StreamName}'",
            _leaderElection.StreamName);

        await _runningCts.CancelAsync();

        if (_publishTask != null)
        {
            try
            {
                await _publishTask;
            }
            catch (OperationCanceledException)
            {
                // Expected when stopping
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Error stopping heartbeat publisher for stream '{StreamName}'",
                    _leaderElection.StreamName);
            }
        }

        _runningCts?.Dispose();
        _runningCts = null;
        _publishTask = null;
    }

    private async Task RunHeartbeatLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug(
            "Heartbeat loop started for stream '{StreamName}'",
            _leaderElection.StreamName);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (_leaderElection.IsLeader)
                {
                    try
                    {
                        // Publish heartbeat
                        await PublishHeartbeatAsync(cancellationToken);

                        // Renew leadership lock
                        var renewed = await _leaderElection.RenewLeadershipAsync(cancellationToken);
                        
                        if (!renewed)
                        {
                            _logger.LogWarning(
                                "Failed to renew leadership for stream '{StreamName}', lost leadership",
                                _leaderElection.StreamName);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex,
                            "Error in heartbeat loop for stream '{StreamName}'",
                            _leaderElection.StreamName);
                    }
                }

                // Wait for next heartbeat interval
                await Task.Delay(_options.HeartbeatInterval, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug(
                "Heartbeat loop cancelled for stream '{StreamName}'",
                _leaderElection.StreamName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Unexpected error in heartbeat loop for stream '{StreamName}'",
                _leaderElection.StreamName);
        }
    }

    private async Task PublishHeartbeatAsync(CancellationToken cancellationToken)
    {
        var heartbeat = new HeartbeatMessage
        {
            LeaderId = _leaderElection.InstanceId,
            Epoch = _leaderElection.CurrentEpoch,
            Timestamp = DateTime.UtcNow,
            StreamName = _leaderElection.StreamName,
            ActiveRequestCount = 0 // TODO: Will be populated by RequestManager later
        };

        var data = _serializer.Serialize(heartbeat);
        
        var subscriberCount = await _redis.PublishAsync(_heartbeatChannel, data, cancellationToken);

        _logger.LogTrace(
            "Published heartbeat for stream '{StreamName}' (epoch {Epoch}) to {SubscriberCount} subscribers",
            _leaderElection.StreamName,
            heartbeat.Epoch,
            subscriberCount);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await StopAsync();
    }
}