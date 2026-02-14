using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Runtime.Channel;
using Streamly.Core.Runtime.Configuration;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Factory implementation for creating leader election services
/// Maintains one instance per stream name
/// </summary>
public class LeaderElectionFactory(
    IRedisConnectionManager redis,
    IMessageSerializer serializer,
    IChannelNameResolver channelResolver,
    IOptions<StreamlyRuntimeOptions> runtimeOptions,
    IOptions<LeaderElectionOptions> leaderElectionOptions,
    ILoggerFactory loggerFactory)
    : ILeaderElectionFactory, IAsyncDisposable
{
    private readonly IRedisConnectionManager _redis = redis ?? throw new ArgumentNullException(nameof(redis));
    private readonly IMessageSerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly IChannelNameResolver _channelResolver = channelResolver ?? throw new ArgumentNullException(nameof(channelResolver));
    private readonly IOptions<StreamlyRuntimeOptions> _runtimeOptions = runtimeOptions ?? throw new ArgumentNullException(nameof(runtimeOptions));
    private readonly IOptions<LeaderElectionOptions> _leaderElectionOptions = leaderElectionOptions ?? throw new ArgumentNullException(nameof(leaderElectionOptions));
    private readonly ILoggerFactory _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
    private readonly ILogger<LeaderElectionFactory> _logger = loggerFactory.CreateLogger<LeaderElectionFactory>();
    
    private readonly ConcurrentDictionary<string, Lazy<StreamLeadershipCoordinator>> _coordinators = new();
    private readonly SemaphoreSlim _startLock = new(1, 1);
    private bool _disposed;

    public ILeaderElectionService GetOrCreate(string streamName)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));
        
        if (_disposed)
            throw new ObjectDisposedException(nameof(LeaderElectionFactory));
        
        var coordinator = _coordinators.GetOrAdd(
            streamName,
            key => new Lazy<StreamLeadershipCoordinator>(() => CreateCoordinator(key))
        ).Value;

        // Ensure coordinator is started (thread-safe, idempotent)
        EnsureCoordinatorStartedAsync(coordinator).GetAwaiter().GetResult();

        return coordinator.LeaderElection;
    }

    private StreamLeadershipCoordinator CreateCoordinator(string streamName)
    {
        _logger.LogDebug(
            "Creating leadership coordinator for stream '{StreamName}'",
            streamName);

        return new StreamLeadershipCoordinator(
            streamName,
            _runtimeOptions.Value.InstanceId,
            _redis,
            _serializer,
            _channelResolver,
            _leaderElectionOptions,
            _loggerFactory);
    }

    private async Task EnsureCoordinatorStartedAsync(StreamLeadershipCoordinator coordinator)
    {
        // Thread-safe start - only one thread can start a coordinator
        await _startLock.WaitAsync();
        try
        {
            // Check if already started (coordinator tracks this internally)
            await coordinator.StartAsync();
        }
        finally
        {
            _startLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        
        _disposed = true;

        _logger.LogInformation(
            "Disposing LeaderElectionFactory, stopping {Count} coordinators",
            _coordinators.Count);

        // Stop and dispose all coordinators
        foreach (var lazyCoordinator in _coordinators.Values)
        {
            if (lazyCoordinator.IsValueCreated)
            {
                try
                {
                    await lazyCoordinator.Value.DisposeAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Error disposing coordinator for stream '{StreamName}'",
                        lazyCoordinator.Value.LeaderElection.StreamName);
                }
            }
        }
        
        _coordinators.Clear();
        _startLock.Dispose();

        _logger.LogInformation("LeaderElectionFactory disposed");
    }
}