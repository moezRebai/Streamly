using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;
using Streamly.Core.Models;
using Streamly.Core.Runtime.Configuration;
using Streamly.Core.Runtime.Context;
using Streamly.Core.Runtime.Leadership;
using Streamly.Core.Runtime.Publishing;
using Streamly.Core.Runtime.Registration;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Core.Runtime.RequestManagement;

/// <summary>
/// Core orchestrator for streaming request lifecycle
/// Handles request opening, coordination, batch sync, and recovery
/// </summary>
internal class RequestManager<TRequest, TResponse> : IRequestManager<TRequest, TResponse>, IAsyncDisposable
{
    private readonly string _streamName;
    private readonly IRequestRegistry<TRequest, TResponse> _registry;
    private readonly IRequestIdentityProvider<TRequest> _identityProvider;
    private readonly IStreamingRequestHandler<TRequest, TResponse> _handler;
    private readonly ILeaderElectionFactory _leaderElectionFactory;
    private ILeaderElectionService _leaderElection = null!;
    private readonly IMessageSerializer _serializer;
    private readonly IOptions<StreamlyRuntimeOptions> _runtimeOptions;
    private readonly IOptions<StateSyncOptions> _stateSyncOptions;
    private readonly ILogger<RequestManager<TRequest, TResponse>> _logger;
    private readonly IStreamingContextFactory<TRequest, TResponse> _contextFactory;
    private readonly IConfirmationQueueFactory _confirmationQueueFactory;
    private ConfirmationQueue? _confirmationQueue;
    private readonly IStreamingTransport _transport;
    private readonly ISubjectResolver _subjects;
    private readonly string _requestsSubject;
    private readonly string _batchSubject;

    private CancellationTokenSource? _batchSyncCts;
    private Task? _batchSyncTask;
    private CancellationTokenSource? _leaseCheckCts;
    private Task? _leaseCheckTask;

    private bool _started;
    private bool _disposed;

    // Maximum number of ActiveRequestSnapshot entries per NATS message.
    // 200 snapshots × ~400 bytes each ≈ 80 KB — well under the 1 MB NATS default.
    // Lower this value if SerializedRequest payloads are large (e.g. IRS with many legs).
    private const int BatchChunkSize = 200;
    
    // Accumulates incoming chunks until a full group is assembled.
    // Key: ChunkGroupId.  Entry removed as soon as the group completes.
    private readonly ConcurrentDictionary<string, ChunkAccumulator> _pendingChunks = new();

    // Epoch du dernier batch accepté par ce follower.
    // Initialisé à -1 (jamais reçu). Un follower n'incrémente jamais son propre epoch
    // (NatsLeaderElection ne le fait qu'à l'acquisition du leadership)
    private long _lastAcceptedBatchEpoch = -1;

    public string StreamName => _streamName;
    public int ActiveRequestCount => _registry.Count;

    private readonly ConcurrentDictionary<string, SubscriberLease> _subscriberLeases = new();

    public RequestManager(
        IStreamRegistry streamRegistry,
        IRequestRegistry<TRequest, TResponse> registry,
        IRequestIdentityProvider<TRequest> identityProvider,
        IStreamingRequestHandler<TRequest, TResponse> handler,
        ILeaderElectionFactory leaderElectionFactory,
        IStreamingTransport transport,
        ISubjectResolver subjects,
        IOptions<StateSyncOptions> stateSyncOptions,
        IOptions<StreamlyRuntimeOptions> runtimeOptions,
        IStreamingContextFactory<TRequest, TResponse> contextFactory,
        IConfirmationPublisherFactory confirmationPublisherFactory,
        IConfirmationQueueFactory confirmationQueueFactory,
        ILoggerFactory loggerFactory,
        IMessageSerializer serializer)
    {
        _streamName = streamRegistry.GetStreamName<TRequest>();
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _identityProvider = identityProvider ?? throw new ArgumentNullException(nameof(identityProvider));
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        _stateSyncOptions = stateSyncOptions ?? throw new ArgumentNullException(nameof(stateSyncOptions));
        _runtimeOptions = runtimeOptions;
        _logger = loggerFactory.CreateLogger<RequestManager<TRequest, TResponse>>();

        _contextFactory = contextFactory ?? throw new ArgumentNullException(nameof(contextFactory));
        _serializer = serializer;
        confirmationPublisherFactory.CreateAsync(_streamName).GetAwaiter().GetResult();
        _confirmationQueueFactory = confirmationQueueFactory ?? throw new ArgumentNullException(nameof(confirmationQueueFactory));
        
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _subjects = subjects ?? throw new ArgumentNullException(nameof(subjects));

        // Stocker la factory — GetOrCreateAsync sera appelé dans StartAsync (fix #1)
        _leaderElectionFactory = leaderElectionFactory
                                 ?? throw new ArgumentNullException(nameof(leaderElectionFactory));

        // Resolve channel names
        _requestsSubject = subjects.GetRequestsSubject(_streamName);
        _batchSubject = subjects.GetBatchSyncSubject(_streamName);

        _logger.LogInformation("RequestManager created for stream '{StreamName}'", _streamName);
    }

    #region Lifecycle Management

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_started)
            return;

        _logger.LogInformation(
            "Starting RequestManager for stream '{StreamName}'",
            _streamName);

        try
        {
            _leaderElection = await _leaderElectionFactory.GetOrCreateAsync(_streamName, cancellationToken);
            _leaderElection.LeadershipChanged += OnLeadershipChanged;

            _confirmationQueue = await _confirmationQueueFactory.CreateAsync(_streamName);
            
            // Subscribe to incoming requests
            await _transport.SubscribeAsync(_requestsSubject, OnRequestReceivedAsync, cancellationToken);

            // Subscribe to batch sync
            await _transport.SubscribeAsync(_batchSubject, OnBatchSyncReceivedAsync, cancellationToken);

            // Subscribe to unsubscribe signals
            var unsubscribeSubject = _subjects.GetUnsubscribeSubject(_streamName);
            await _transport.SubscribeAsync(unsubscribeSubject, OnUnsubscribeReceivedAsync, cancellationToken);

            // Subscribe to close events from leader  ← NEW
            var eventsSubject = _subjects.GetCloseEventsSubject(_streamName);
            await _transport.SubscribeAsync(eventsSubject, OnCloseEventReceivedAsync, cancellationToken);

            var heartbeatSubject = _subjects.GetSubscriberHeartbeatSubject();
            await _transport.SubscribeAsync(
                heartbeatSubject,
                OnSubscriberHeartbeatReceivedAsync,
                cancellationToken);

            // Start batch sync loop
            _batchSyncCts = new CancellationTokenSource();
            _batchSyncTask = RunBatchSyncLoopAsync(_batchSyncCts.Token);

            _confirmationQueue.Start(_batchSyncCts!.Token);

            // Start lease check loop
            _leaseCheckCts = new CancellationTokenSource();
            _leaseCheckTask = RunLeaseCheckLoopAsync(_leaseCheckCts.Token);

            _started = true;

            _logger.LogInformation(
                "RequestManager started for stream '{StreamName}'",
                _streamName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to start RequestManager for stream '{StreamName}'",
                _streamName);
            throw;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (!_started)
            return;

        _logger.LogInformation(
            "Stopping RequestManager for stream '{StreamName}'",
            _streamName);

        try
        {
            // Stop batch sync loop
            await _batchSyncCts?.CancelAsync()!;
            await _leaseCheckCts?.CancelAsync()!;

            if (_batchSyncTask != null)
            {
                try
                {
                    await _batchSyncTask;
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            }

            if (_leaseCheckTask != null)
            {
                try
                {
                    await _leaseCheckTask;
                }
                catch (OperationCanceledException)
                {
                }
            }

            if (_confirmationQueue is not null)
            {
                await _confirmationQueue.StopAsync();
            }

            // Close all active requests
            await CloseAllRequestsAsync(CloseReason.Shutdown, cancellationToken);

            // Unsubscribe from channels
            await _transport.UnsubscribeAsync(_requestsSubject, cancellationToken);
            await _transport.UnsubscribeAsync(_batchSubject, cancellationToken);

            _started = false;

            _logger.LogInformation(
                "RequestManager stopped for stream '{StreamName}'",
                _streamName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error stopping RequestManager for stream '{StreamName}'",
                _streamName);
            throw;
        }
    }

    #endregion

    #region Request Opening

    public Task<string> OpenRequestAsync(
        RequestEnvelope<TRequest> envelope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        try
        {
            // Compute deterministic request ID (includes StreamBehavior)
            var requestId = _identityProvider.ComputeRequestId(
                envelope.Request,
                envelope.Behavior);

            _logger.LogDebug(
                "Processing {Behavior} request with ID '{RequestId}' for stream '{StreamName}'",
                envelope.Behavior,
                requestId,
                _streamName);

            // Check if already exists (idempotent)
            if (_registry.TryGet(requestId, out var existing))
            {
                _logger.LogDebug(
                    "Request '{RequestId}' already exists (subscribers: {Count}), incrementing",
                    requestId,
                    existing!.SubscriberCount);

                _registry.TryUpdate(requestId, metadata =>
                {
                    metadata.SubscriberCount++;
                    metadata.LastUpdateAt = DateTime.UtcNow;
                });

                TrackSubscriberLease(envelope.SubscriberId, requestId);

                return Task.FromResult(requestId);
            }

            // Nouveau request : construire les metadata avec SubscriberCount = 1
            var metadata = new RequestMetadata<TRequest, TResponse>
            {
                RequestId = requestId,
                Request = envelope.Request,
                SerializedRequest = _identityProvider.Serialize(envelope.Request),
                StreamBehavior = envelope.Behavior, // ← ADDED
                State = RequestState.Registered,
                SubscriberCount = 1,
                OpenedAt = DateTime.UtcNow,
                LastUpdateAt = DateTime.UtcNow,
                Epoch = _leaderElection.CurrentEpoch,
                LatestImage = default
            };

            // Ajouter au registry
            if (_registry.TryAdd(requestId, metadata))
            {
                _logger.LogInformation(
                    "Opened new {Behavior} request '{RequestId}' for stream '{StreamName}' (total: {Count})",
                    envelope.Behavior,
                    requestId,
                    _streamName,
                    _registry.Count);

                _registry.TryUpdate(requestId, m => m.State = RequestState.Streaming);

                // Tracker le lease du subscriber sur le chemin normal
                TrackSubscriberLease(envelope.SubscriberId, requestId);

                // Invoke handler (runs indefinitely for Live streams)
                // Fire-and-forget: don't await - handler runs in background
                _ = InvokeHandlerOpenedAsync(
                    envelope.Request,
                    requestId,
                    envelope.Behavior,
                    metadata.HandlerCts.Token);
            }
            else
            {
                // Race condition: un autre thread a ajouté le request entre le TryGet et le TryAdd
                _registry.TryUpdate(requestId, m =>
                {
                    m.SubscriberCount++;
                    m.LastUpdateAt = DateTime.UtcNow;
                });
                TrackSubscriberLease(envelope.SubscriberId, requestId);
            }

            return Task.FromResult(requestId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error opening request for stream '{StreamName}'",
                _streamName);
            throw;
        }
    }

    private void TrackSubscriberLease(string subscriberId, string requestId)
    {
        if (string.IsNullOrWhiteSpace(subscriberId))
            return;

        var lease = _subscriberLeases.GetOrAdd(subscriberId, id => new SubscriberLease
        {
            SubscriberId = id
        });

        lock (lease)
        {
            lease.RequestIds.Add(requestId);
            lease.LastHeartbeat = DateTime.UtcNow;
        }
    }

    private async Task InvokeHandlerOpenedAsync(
        TRequest request,
        string requestId,
        StreamBehavior behavior, // ← ADDED
        CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogDebug(
                "Invoking handler.OnRequestOpenedAsync for {Behavior} request '{RequestId}'",
                behavior,
                requestId);

            // Create context with behavior
            var context = _contextFactory.Create(requestId, behavior); // ← Pass behavior

            await _handler.OnRequestOpenedAsync(request, context, cancellationToken);

            _logger.LogDebug(
                "Handler.OnRequestOpenedAsync completed for request '{RequestId}'",
                requestId);
        }
        catch (OperationCanceledException)
        {
            // Expected — request was closed, handler loop cancelled
            _logger.LogDebug(
                "Handler loop cancelled for request '{RequestId}'", requestId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Handler.OnRequestOpenedAsync failed for request '{RequestId}'",
                requestId);

            _registry.TryUpdate(requestId, m => m.State = RequestState.Closing);
            throw;
        }
    }

    #endregion

    #region Request Closing

    public async Task CloseRequestAsync(string requestId, CloseReason reason,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(requestId))
            throw new ArgumentException("Request ID cannot be null or whitespace", nameof(requestId));

        try
        {
            if (!_registry.TryGet(requestId, out var metadata) || metadata == null)
            {
                _logger.LogDebug(
                    "Cannot close request '{RequestId}' - not found in registry",
                    requestId);
                return;
            }

            _logger.LogInformation(
                "Closing request '{RequestId}' with reason: {Reason}",
                requestId,
                reason);

            // Update state to Closing
            _registry.TryUpdate(requestId, m => m.State = RequestState.Closing);

            // Cancel handler loop BEFORE invoking OnRequestClosingAsync
            await metadata.HandlerCts.CancelAsync();

            // Invoke handler cleanup
            try
            {
                await _handler.OnRequestClosingAsync(metadata!.Request, reason, cancellationToken);

                _logger.LogDebug(
                    "Handler.OnRequestClosingAsync completed for request '{RequestId}'",
                    requestId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Handler.OnRequestClosingAsync failed for request '{RequestId}'",
                    requestId);
            }

            // Remove from registry
            if (_registry.TryRemove(requestId, out _))
            {
                _logger.LogInformation(
                    "Closed request '{RequestId}', reason: {Reason} (remaining: {Count})",
                    requestId,
                    reason,
                    _registry.Count);
            }

            metadata.HandlerCts.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error closing request '{RequestId}'",
                requestId);
            throw;
        }
    }

    private async Task CloseAllRequestsAsync(CloseReason reason, CancellationToken cancellationToken)
    {
        var requestIds = _registry.GetAllRequestIds().ToList();

        _logger.LogInformation(
            "Closing all {Count} requests for stream '{StreamName}' with reason: {Reason}",
            requestIds.Count,
            _streamName,
            reason);

        foreach (var requestId in requestIds)
        {
            try
            {
                await CloseRequestAsync(requestId, reason, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Error closing request '{RequestId}' during shutdown",
                    requestId);
            }
        }
    }

    private async Task OnCloseEventReceivedAsync(byte[] data)
    {
        try
        {
            var closeEvent = _serializer.Deserialize<RequestClosedEvent>(data);

            if (closeEvent.StreamName != _streamName)
                return;

            // Validate epoch
            if (closeEvent.Epoch < _leaderElection.CurrentEpoch)
            {
                _logger.LogWarning(
                    "Ignoring stale close event for request '{RequestId}' (epoch {EventEpoch} < {CurrentEpoch})",
                    closeEvent.RequestId,
                    closeEvent.Epoch,
                    _leaderElection.CurrentEpoch);
                return;
            }

            _logger.LogInformation(
                "Received close event for request '{RequestId}', reason: {Reason}",
                closeEvent.RequestId,
                closeEvent.Reason);

            // Close locally (invokes handler cleanup + removes from registry)
            await CloseRequestAsync(closeEvent.RequestId, closeEvent.Reason, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error processing close event for stream '{StreamName}'",
                _streamName);
        }
    }

    private Task OnSubscriberHeartbeatReceivedAsync(byte[] data)
    {
        try
        {
            // Only leader tracks subscriber leases
            if (!_leaderElection.IsLeader)
                return Task.CompletedTask;

            var heartbeat = _serializer.Deserialize<SubscriberHeartbeatMessage>(data);

            if (string.IsNullOrWhiteSpace(heartbeat.SubscriberId))
                return Task.CompletedTask;

            if (_subscriberLeases.TryGetValue(heartbeat.SubscriberId, out var lease))
            {
                lock (lease)
                {
                    lease.LastHeartbeat = DateTime.UtcNow;
                }

                _logger.LogTrace(
                    "Heartbeat received from subscriber '{SubscriberId}'",
                    heartbeat.SubscriberId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing subscriber heartbeat");
        }

        return Task.CompletedTask;
    }

    #endregion²

    #region Batch Sync - Publishing (Leader)

    private async Task RunBatchSyncLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug(
            "Batch sync loop started for stream '{StreamName}'",
            _streamName);

        var interval = _stateSyncOptions.Value.BatchSyncInterval;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Only publish if we're the leader
                if (_leaderElection.IsLeader)
                {
                    try
                    {
                        await PublishBatchSyncAsync(cancellationToken);
                        //await CheckExpiredSubscriberLeasesAsync(cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex,
                            "Error publishing batch sync for stream '{StreamName}'",
                            _streamName);
                    }
                }

                await Task.Delay(interval, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug(
                "Batch sync loop cancelled for stream '{StreamName}'",
                _streamName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Unexpected error in batch sync loop for stream '{StreamName}'",
                _streamName);
        }
    }

    // In RequestManager.cs - update snapshot building to include StreamBehavior
    private async Task PublishBatchSyncAsync(CancellationToken cancellationToken)
    {
        try
        {
            var allRequests = _registry.GetAll().ToList();

            _logger.LogInformation(
                "Publishing batch sync for stream '{StreamName}' with {Count} requests",
                _streamName, allRequests.Count);

            var snapshots = allRequests.Select(m => new ActiveRequestSnapshot
            {
                RequestId = m.RequestId,
                SerializedRequest = m.SerializedRequest,
                StreamBehavior = m.StreamBehavior,
                State = m.State,
                SubscriberCount = m.SubscriberCount,
                OpenedAt = m.OpenedAt,
                LastUpdateAt = m.LastUpdateAt
            }).ToList();

            var chunks = Chunk(snapshots, BatchChunkSize);
            var totalChunks = chunks.Count;
            var groupId = Guid.NewGuid().ToString("N");
            var epoch = _leaderElection.CurrentEpoch;
            var timestamp = DateTime.UtcNow;

            for (var i = 0; i < totalChunks; i++)
            {
                var message = new BatchSyncMessage
                {
                    Epoch = epoch,
                    Timestamp = timestamp,
                    StreamName = _streamName,
                    ChunkGroupId = groupId,
                    ChunkIndex = i,
                    TotalChunks = totalChunks,
                    ActiveRequests = chunks[i]
                };

                var data = _serializer.Serialize(message);
                await _transport.PublishAsync(_batchSubject, data, cancellationToken);
            }

            _logger.LogDebug(
                "Published batch sync for stream '{StreamName}': {Count} requests across {Chunks} chunk(s)",
                _streamName, snapshots.Count, totalChunks);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error publishing batch sync for stream '{StreamName}'", _streamName);
            throw;
        }
    }

    /// <summary>
    /// Splits <paramref name="source"/> into pages of at most <paramref name="size"/> elements.
    /// Always returns at least one page (possibly empty) so TotalChunks is never 0.
    /// </summary>
    private static List<List<T>> Chunk<T>(List<T> source, int size)
    {
        var pages = new List<List<T>>();
        for (var i = 0; i < source.Count; i += size)
            pages.Add(source.GetRange(i, Math.Min(size, source.Count - i)));

        if (pages.Count == 0)
            pages.Add([]);

        return pages;
    }

    #endregion

    #region Batch Sync - Receiving (All Instances)

    private async Task OnBatchSyncReceivedAsync(byte[] data)
    {
        try
        {
            var batch = _serializer.Deserialize<BatchSyncMessage>(data);

            if (batch.StreamName != _streamName)
                return;

            if (_leaderElection.IsLeader)
            {
                _logger.LogTrace(
                    "Skipping batch sync for stream '{StreamName}' — we are the leader",
                    _streamName);
                return;
            }

            // Stale-epoch guard (unchanged).
            if (batch.Epoch < _lastAcceptedBatchEpoch)
            {
                _logger.LogWarning(
                    "Ignoring stale batch sync (batch epoch {BatchEpoch} < last accepted {LastAccepted})",
                    batch.Epoch, _lastAcceptedBatchEpoch);
                return;
            }

            // ── chunk assembly ───────────────────────────────────────────────────
            // Normalise: legacy messages have TotalChunks == 0 (field absent in JSON).
            var totalChunks = batch.TotalChunks < 1 ? 1 : batch.TotalChunks;

            List<ActiveRequestSnapshot> allSnapshots;

            if (totalChunks == 1)
            {
                // Fast path — single chunk or legacy single-message batch.
                allSnapshots = batch.ActiveRequests;
            }
            else
            {
                // Multi-chunk path — accumulate until the group is complete.
                var accumulator = _pendingChunks.GetOrAdd(
                    batch.ChunkGroupId,
                    _ => new ChunkAccumulator(batch.Epoch, totalChunks));

                if (!accumulator.TryComplete(batch.ChunkIndex, batch.ActiveRequests))
                {
                    _logger.LogDebug(
                        "Received chunk {Received}/{Total} for group '{GroupId}'",
                        batch.ChunkIndex + 1, totalChunks, batch.ChunkGroupId);
                    return; // waiting for remaining chunks
                }

                allSnapshots = accumulator.Flatten();
                _pendingChunks.TryRemove(batch.ChunkGroupId, out _);

                _logger.LogDebug(
                    "Assembled {Total} chunks for group '{GroupId}' — {Count} requests",
                    totalChunks, batch.ChunkGroupId, allSnapshots.Count);
            }

            // ── reconcile (logic unchanged, now operates on full assembled list) ─
            _lastAcceptedBatchEpoch = batch.Epoch;

            _logger.LogInformation(
                "Processing batch sync for stream '{StreamName}' with {Count} requests (epoch {Epoch})",
                _streamName, allSnapshots.Count, batch.Epoch);

            foreach (var snapshot in allSnapshots)
                await ReconcileRequestAsync(snapshot, CancellationToken.None);

            await DetectOrphanedRequestsAsync(allSnapshots, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error processing batch sync for stream '{StreamName}'", _streamName);
        }
    }

    private async Task ReconcileRequestAsync(ActiveRequestSnapshot snapshot, CancellationToken cancellationToken)
    {
        try
        {
            if (_registry.TryGet(snapshot.RequestId, out _))
            {
                // Request exists locally - reconcile state
                _logger.LogTrace(
                    "Request '{RequestId}' exists locally, reconciling state",
                    snapshot.RequestId);

                _registry.TryUpdate(snapshot.RequestId, metadata =>
                {
                    metadata.SubscriberCount = snapshot.SubscriberCount;
                    metadata.State = snapshot.State;
                    metadata.Epoch = _leaderElection.CurrentEpoch;
                });
            }
            else
            {
                // Request is MISSING locally - RECOVER IT
                _logger.LogWarning(
                    "Request '{RequestId}' missing locally, recovering from batch sync",
                    snapshot.RequestId);

                await RecoverMissingRequestAsync(snapshot, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error reconciling request '{RequestId}'",
                snapshot.RequestId);
        }
    }

    private async Task RecoverMissingRequestAsync(
        ActiveRequestSnapshot snapshot,
        CancellationToken cancellationToken)
    {
        try
        {
            // Deserialize the full request
            var request = _identityProvider.Deserialize(snapshot.SerializedRequest);

            _logger.LogInformation(
                "Recovering missing {Behavior} request '{RequestId}' for stream '{StreamName}'",
                snapshot.StreamBehavior,
                snapshot.RequestId,
                _streamName);

            // Create metadata - now includes StreamBehavior
            var metadata = new RequestMetadata<TRequest, TResponse>
            {
                RequestId = snapshot.RequestId,
                Request = request,
                SerializedRequest = snapshot.SerializedRequest,
                StreamBehavior = snapshot.StreamBehavior, // ← ADDED
                State = snapshot.State,
                SubscriberCount = snapshot.SubscriberCount,
                OpenedAt = snapshot.OpenedAt,
                LastUpdateAt = snapshot.LastUpdateAt,
                Epoch = _leaderElection.CurrentEpoch,
                LatestImage = default
            };

            // Add to registry
            if (_registry.TryAdd(snapshot.RequestId, metadata))
            {
                _logger.LogInformation(
                    "Successfully recovered request '{RequestId}', invoking handler",
                    snapshot.RequestId);

                // Invoke handler with StreamBehavior  ← FIXED
                await InvokeHandlerOpenedAsync(
                    request,
                    snapshot.RequestId,
                    snapshot.StreamBehavior, // ← ADDED
                    cancellationToken);

                _logger.LogInformation(
                    "Handler invoked for recovered {Behavior} request '{RequestId}'",
                    snapshot.StreamBehavior,
                    snapshot.RequestId);
            }
            else
            {
                // Race condition: another thread added it meanwhile
                _logger.LogDebug(
                    "Request '{RequestId}' was added by another thread during recovery",
                    snapshot.RequestId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to recover request '{RequestId}' from batch sync",
                snapshot.RequestId);
        }
    }

    private async Task DetectOrphanedRequestsAsync(
        List<ActiveRequestSnapshot> batchRequests,
        CancellationToken cancellationToken)
    {
        try
        {
            var localRequestIds = _registry.GetAllRequestIds().ToHashSet();
            var batchRequestIds = batchRequests.Select(r => r.RequestId).ToHashSet();
            var orphanedIds = localRequestIds.Except(batchRequestIds).ToList();

            if (orphanedIds.Any())
            {
                _logger.LogWarning(
                    "Detected {Count} orphaned requests not in leader's batch sync",
                    orphanedIds.Count);

                foreach (var orphanedId in orphanedIds)
                {
                    _logger.LogWarning(
                        "Closing orphaned request '{RequestId}' (not in leader's state)",
                        orphanedId);

                    await CloseRequestAsync(orphanedId, CloseReason.Orphaned, cancellationToken);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error detecting orphaned requests for stream '{StreamName}'",
                _streamName);
        }
    }

    private void RemoveFromSubscriberLease(string subscriberId, string requestId)
    {
        if (string.IsNullOrWhiteSpace(subscriberId))
            return;

        if (!_subscriberLeases.TryGetValue(subscriberId, out var lease))
            return;

        lock (lease)
        {
            lease.RequestIds.Remove(requestId);

            // Remove lease entirely if no more active streams
            if (lease.RequestIds.Count == 0)
                _subscriberLeases.TryRemove(subscriberId, out _);
        }
    }

    private async Task CheckExpiredSubscriberLeasesAsync(CancellationToken cancellationToken)
    {
        var threshold = _runtimeOptions.Value.SubscriberHeartbeatTimeoutMs;
        var now = DateTime.UtcNow;

        foreach (var (subscriberId, lease) in _subscriberLeases)
        {
            DateTime lastHeartbeat;
            List<string> requestIds;

            lock (lease)
            {
                lastHeartbeat = lease.LastHeartbeat;
                requestIds = lease.RequestIds.ToList();
            }

            if (now - lastHeartbeat < TimeSpan.FromMilliseconds(threshold))
                continue;

            _logger.LogWarning(
                "Subscriber '{SubscriberId}' lease expired (last heartbeat: {LastHeartbeat}), " +
                "closing {Count} stream(s)",
                subscriberId,
                lastHeartbeat,
                requestIds.Count);

            // Remove lease first to avoid double-processing
            _subscriberLeases.TryRemove(subscriberId, out _);

            foreach (var requestId in requestIds)
            {
                if (!_registry.TryGet(requestId, out var metadata) || metadata == null)
                    continue;

                var shouldClose = false;

                _registry.TryUpdate(requestId, m =>
                {
                    m.SubscriberCount = Math.Max(0, m.SubscriberCount - 1);

                    _logger.LogInformation(
                        "Decremented subscriber count for request '{RequestId}': {Count} " +
                        "(lease expiry for '{SubscriberId}')",
                        requestId, m.SubscriberCount, subscriberId);

                    if (m is { StreamBehavior: StreamBehavior.Live, SubscriberCount: 0 })
                        shouldClose = true;
                });

                if (shouldClose)
                {
                    await CloseRequestAsync(
                        requestId,
                        CloseReason.Timeout,
                        cancellationToken);
                }
            }
        }
    }

    private async Task RunLeaseCheckLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug(
            "Lease check loop started for stream '{StreamName}'",
            _streamName);

        // Check every 1/3 of timeout — guarantees ghost detected
        // within timeout + one check interval at most
        var interval = TimeSpan.FromMilliseconds(
            _runtimeOptions.Value.SubscriberHeartbeatTimeoutMs / 3.0);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(interval, cancellationToken);

                // Only leader manages subscriber counts and stream lifecycle
                if (!_leaderElection.IsLeader)
                    continue;

                try
                {
                    await CheckExpiredSubscriberLeasesAsync(cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Error checking expired leases for stream '{StreamName}'",
                        _streamName);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug(
                "Lease check loop cancelled for stream '{StreamName}'",
                _streamName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Unexpected error in lease check loop for stream '{StreamName}'",
                _streamName);
        }
    }

    #endregion

    #region Incoming Requests (Redis Channel)

    private async Task OnRequestReceivedAsync(byte[] data)
    {
        try
        {
            // Deserialize envelope (wraps user request + behavior + correlationId)
            var envelope = _serializer.Deserialize<RequestEnvelope<TRequest>>(data);

            _logger.LogDebug(
                "Received {Behavior} request for stream '{StreamName}'",
                envelope.Behavior,
                _streamName);

            // Open request (idempotent - same request = same RequestId)
            // ALL instances do this (for failover readiness)
            var requestId = await OpenRequestAsync(envelope, CancellationToken.None);
            
            // ONLY LEADER confirms — enqueue and return immediately.
            // ConfirmationQueue drain loop sends at NATS pace, one at a time.
            if (_leaderElection.IsLeader)
            {
                await _confirmationQueue!.EnqueueAsync(
                    envelope.CorrelationId,
                    requestId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing incoming request for stream '{StreamName}'", _streamName);
        }
    }

    private async Task OnUnsubscribeReceivedAsync(byte[] data)
    {
        try
        {
            var envelope = _serializer.Deserialize<UnsubscribeEnvelope>(data);

            if (envelope.StreamName != _streamName)
                return;

            _logger.LogDebug(
                "Received unsubscribe for request '{RequestId}'",
                envelope.RequestId);

            if (!_registry.TryGet(envelope.RequestId, out var metadata) || metadata == null)
            {
                _logger.LogDebug(
                    "Unsubscribe for unknown request '{RequestId}', ignoring",
                    envelope.RequestId);
                return;
            }

            // Only leader manages subscriber count and auto-close
            if (!_leaderElection.IsLeader)
                return;

            var shouldClose = false;

            _registry.TryUpdate(envelope.RequestId, m =>
            {
                m.SubscriberCount = Math.Max(0, m.SubscriberCount - 1);

                _logger.LogInformation(
                    "Decremented subscriber count for request '{RequestId}': {Count}",
                    envelope.RequestId,
                    m.SubscriberCount);

                // Auto-close Live stream when no subscribers
                if (m is { StreamBehavior: StreamBehavior.Live, SubscriberCount: 0 })
                {
                    shouldClose = true;
                }
            });

            RemoveFromSubscriberLease(envelope.SubscriberId, envelope.RequestId);

            if (shouldClose)
            {
                _logger.LogInformation(
                    "No subscribers remaining for Live request '{RequestId}', auto-closing",
                    envelope.RequestId);

                await CloseRequestAsync(
                    envelope.RequestId,
                    CloseReason.Unsubscribe,
                    CancellationToken.None);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error processing unsubscribe for stream '{StreamName}'",
                _streamName);
        }
    }

    #endregion

    #region Query Methods

    public RequestMetadata<TRequest, TResponse>? GetRequest(string requestId)
    {
        _registry.TryGet(requestId, out var metadata);
        return metadata;
    }

    public string[] GetActiveRequestIds()
    {
        return _registry.GetAllRequestIds().ToArray();
    }

    #endregion

    #region Leadership Events

    private void OnLeadershipChanged(object? sender, LeadershipChangedEventArgs e)
    {
        _logger.LogInformation(
            "Leadership changed for stream '{StreamName}': {PreviousState} → {NewState} (epoch {Epoch})",
            _streamName,
            e.PreviousState,
            e.NewState,
            e.Epoch);

        // If we became leader, trigger immediate batch sync
        if (e.NewState == LeadershipState.Leader)
        {
            _ = Task.Run(async () =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100)); // Small delay to stabilize
                await PublishBatchSyncAsync(CancellationToken.None);
            });
        }
    }

    #endregion

    #region Disposal

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        _leaderElection.LeadershipChanged -= OnLeadershipChanged;

        await StopAsync();

        _batchSyncCts?.Dispose();

        _leaseCheckCts?.Dispose();

        if (_confirmationQueue is not null)
        {
            await _confirmationQueue.DisposeAsync();
        }
        
        _logger.LogDebug(
            "RequestManager disposed for stream '{StreamName}'",
            _streamName);
    }

    #endregion
}