using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Runtime.Configuration;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Coordinates all leadership components for a single stream
/// - LeaderElectionService (manages leadership state)
/// - HeartbeatPublisher (publishes heartbeats when leader)
/// - LeaderMonitor (monitors leader health and triggers failover)
/// 
/// MIGRATION NOTE: Replaced IRedisConnectionManager → IStreamingTransport
///                 Replaced IChannelNameResolver → ISubjectResolver
///                 Injects ILeaderElection from Infrastructure
/// </summary>
internal class StreamLeadershipCoordinator : IAsyncDisposable
{
    private readonly ILogger<StreamLeadershipCoordinator> _logger;
    private bool _disposed;
    private bool _started;

    public ILeaderElectionService LeaderElection { get; }
    
    private readonly HeartbeatPublisher _heartbeatPublisher;
    private readonly LeaderMonitor _leaderMonitor;

    public StreamLeadershipCoordinator(
        string streamName,
        string instanceId,
        ILeaderElection infrastructureLeaderElection,
        IStreamingTransport transport,
        IMessageSerializer serializer,
        ISubjectResolver subjects,
        IOptions<LeaderElectionOptions> options,
        ILoggerFactory loggerFactory)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));
        if (string.IsNullOrWhiteSpace(instanceId))
            throw new ArgumentException("Instance ID cannot be null or whitespace", nameof(instanceId));
        
        _logger = loggerFactory.CreateLogger<StreamLeadershipCoordinator>();

        // Create leader election service (wraps Infrastructure's ILeaderElection)
        LeaderElection = new LeaderElectionService(
            streamName,
            instanceId,
            infrastructureLeaderElection,
            loggerFactory.CreateLogger<LeaderElectionService>());

        // Create heartbeat publisher
        _heartbeatPublisher = new HeartbeatPublisher(
            LeaderElection,
            transport,
            serializer,
            subjects,
            options,
            loggerFactory.CreateLogger<HeartbeatPublisher>());

        // Create leader monitor
        _leaderMonitor = new LeaderMonitor(
            LeaderElection,
            options,
            subjects,
            transport,
            loggerFactory.CreateLogger<LeaderMonitor>());

        _logger.LogDebug(
            "StreamLeadershipCoordinator created for stream '{StreamName}' with instance '{InstanceId}'",
            streamName,
            instanceId);
    }

    /// <summary>
    /// Start all leadership components
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_started)
        {
            _logger.LogWarning(
                "StreamLeadershipCoordinator already started for stream '{StreamName}'",
                LeaderElection.StreamName);
            return;
        }

        _logger.LogInformation(
            "Starting leadership coordination for stream '{StreamName}'",
            LeaderElection.StreamName);

        try
        {
            // Start in order:
            // 1. Leader election (subscribes to heartbeats, attempts initial acquisition)
            await LeaderElection.StartAsync(cancellationToken);

            // 2. Heartbeat publisher (starts publishing if leader)
            await _heartbeatPublisher.StartAsync(cancellationToken);

            // 3. Leader monitor (monitors for dead leader)
            await _leaderMonitor.StartAsync(cancellationToken);

            _started = true;

            _logger.LogInformation(
                "Leadership coordination started for stream '{StreamName}', state: {State}",
                LeaderElection.StreamName,
                LeaderElection.State);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to start leadership coordination for stream '{StreamName}'",
                LeaderElection.StreamName);
            
            // Cleanup on failure
            await StopAsync(cancellationToken);
            throw;
        }
    }

    /// <summary>
    /// Stop all leadership components
    /// </summary>
    private async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (!_started)
            return;

        _logger.LogInformation(
            "Stopping leadership coordination for stream '{StreamName}'",
            LeaderElection.StreamName);

        try
        {
            // Stop in reverse order:
            // 1. Leader monitor (stop checking for dead leader)
            await _leaderMonitor.StopAsync(cancellationToken);

            // 2. Heartbeat publisher (stop publishing heartbeats)
            await _heartbeatPublisher.StopAsync(cancellationToken);

            // 3. Leader election (release leadership if held)
            await LeaderElection.StopAsync(cancellationToken);

            _started = false;

            _logger.LogInformation(
                "Leadership coordination stopped for stream '{StreamName}'",
                LeaderElection.StreamName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error stopping leadership coordination for stream '{StreamName}'",
                LeaderElection.StreamName);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        await StopAsync();

        await _leaderMonitor.DisposeAsync();
        await _heartbeatPublisher.DisposeAsync();
        LeaderElection.Dispose();

        _logger.LogDebug(
            "StreamLeadershipCoordinator disposed for stream '{StreamName}'",
            LeaderElection.StreamName);
    }
}