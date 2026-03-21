using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;
using Streamly.Infrastructure.Interfaces;
using Streamly.Infrastructure.Nats;

namespace Streamly.Server.Leadership;

/// <summary>
/// Factory implementation for creating leader election services.
/// Maintains one instance per stream name.
/// </summary>
public class LeaderElectionFactory(
    NatsConnectionManager transport,
    IOptions<NatsConnectionOptions> natsOptions,
    IMessageSerializer serializer,
    ISubjectResolver subjects,
    IOptions<StreamlySettings> options,
    ILoggerFactory loggerFactory)
    : ILeaderElectionFactory, IAsyncDisposable
{
    private readonly NatsConnectionManager _transport = transport ?? throw new ArgumentNullException(nameof(transport));
    private readonly IOptions<NatsConnectionOptions> _natsOptions = natsOptions ?? throw new ArgumentNullException(nameof(natsOptions));
    private readonly IMessageSerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ISubjectResolver _subjectResolver = subjects ?? throw new ArgumentNullException(nameof(subjects));
    private readonly IOptions<StreamlySettings> _options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly ILoggerFactory _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
    private readonly ILogger<LeaderElectionFactory> _logger = loggerFactory.CreateLogger<LeaderElectionFactory>();

    private readonly ConcurrentDictionary<string, Lazy<StreamLeadershipCoordinator>> _coordinators = new();
    private readonly SemaphoreSlim _startLock = new(1, 1);
    private bool _disposed;

    public async Task<ILeaderElectionService> GetOrCreateAsync(
        string streamName,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));

        if (_disposed)
            throw new ObjectDisposedException(nameof(LeaderElectionFactory));

        var coordinator = _coordinators.GetOrAdd(
            streamName,
            key => new Lazy<StreamLeadershipCoordinator>(() => CreateCoordinator(key))
        ).Value;

        await EnsureCoordinatorStartedAsync(coordinator, cancellationToken);

        return coordinator.LeaderElection;
    }

    private StreamLeadershipCoordinator CreateCoordinator(string streamName)
    {
        _logger.LogDebug(
            "Creating leadership coordinator for stream '{StreamName}'",
            streamName);

        var streamLeaderElection = new NatsLeaderElection(
            _transport,
            _natsOptions,
            _options,
            _subjectResolver,
            _loggerFactory.CreateLogger<NatsLeaderElection>(),
            streamName);

        return new StreamLeadershipCoordinator(
            streamName,
            _options.Value.InstanceId,
            streamLeaderElection,
            _transport,
            _serializer,
            _subjectResolver,
            _options,
            _loggerFactory);
    }

    private async Task EnsureCoordinatorStartedAsync(
        StreamLeadershipCoordinator coordinator,
        CancellationToken cancellationToken)
    {
        await _startLock.WaitAsync(cancellationToken);
        try
        {
            await coordinator.StartAsync(cancellationToken);
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
