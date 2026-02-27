using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Runtime.Configuration;
using Streamly.Infrastructure.Interfaces;
using Streamly.Infrastructure.Nats;

namespace Streamly.Core.Runtime.Leadership;

/// <summary>
/// Factory implementation for creating leader election services.
/// Maintains one instance per stream name.
/// 
/// CRITICAL: Each stream gets its own NatsLeaderElection instance with stream-specific key.
/// This enables independent leadership per stream (load distribution + fault isolation).
/// </summary>
public class LeaderElectionFactory : ILeaderElectionFactory, IAsyncDisposable
{
    // ✅ CHANGED: Instead of injecting ILeaderElection, inject components to CREATE them
    private readonly NatsConnectionManager _transport;
    private readonly IOptions<NatsConnectionOptions> _natsOptions;
    private readonly IMessageSerializer _serializer;
    private readonly ISubjectResolver _subjects;
    private readonly IOptions<StreamlyRuntimeOptions> _runtimeOptions;
    private readonly IOptions<LeaderElectionOptions> _leaderElectionOptions;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<LeaderElectionFactory> _logger;
    
    private readonly ConcurrentDictionary<string, Lazy<StreamLeadershipCoordinator>> _coordinators = new();
    private readonly SemaphoreSlim _startLock = new(1, 1);
    private bool _disposed;

    public LeaderElectionFactory(
        NatsConnectionManager transport,                      // ✅ CHANGED: Direct dependency, not IStreamingTransport
        IOptions<NatsConnectionOptions> natsOptions,          // ✅ NEW: Need NATS options
        IMessageSerializer serializer,
        ISubjectResolver subjects,
        IOptions<StreamlyRuntimeOptions> runtimeOptions,
        IOptions<LeaderElectionOptions> leaderElectionOptions,
        ILoggerFactory loggerFactory)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _natsOptions = natsOptions ?? throw new ArgumentNullException(nameof(natsOptions));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _subjects = subjects ?? throw new ArgumentNullException(nameof(subjects));
        _runtimeOptions = runtimeOptions ?? throw new ArgumentNullException(nameof(runtimeOptions));
        _leaderElectionOptions = leaderElectionOptions ?? throw new ArgumentNullException(nameof(leaderElectionOptions));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _logger = loggerFactory.CreateLogger<LeaderElectionFactory>();
    }

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

        // ✅ CRITICAL FIX: Create a NEW NatsLeaderElection for THIS stream
        var streamLeaderElection = new NatsLeaderElection(
            _transport,
            _natsOptions,
            _loggerFactory.CreateLogger<NatsLeaderElection>(),
            streamName);  // ← Stream-specific instance!

        return new StreamLeadershipCoordinator(
            streamName,
            _runtimeOptions.Value.InstanceId,
            streamLeaderElection,  // ← Pass stream-specific instance
            _transport,
            _serializer,
            _subjects,
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