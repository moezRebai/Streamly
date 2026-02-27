using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
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
    private readonly ILeaderElectionService _leaderElection;
    private readonly IMessageSerializer _serializer;
    private readonly IOptions<StateSyncOptions> _stateSyncOptions;
    private readonly ILogger<RequestManager<TRequest, TResponse>> _logger;
    private readonly IStreamingContextFactory<TRequest, TResponse> _contextFactory;
    private readonly ConfirmationPublisher _confirmationPublisher;
    private readonly IStreamingTransport _transport;
    private readonly ISubjectResolver _subjects;
    private readonly string _requestsSubject;
    private readonly string _batchSubject;

    private CancellationTokenSource? _batchSyncCts;
    private Task? _batchSyncTask;
    private long _currentEpoch;
    private bool _started;
    private bool _disposed;

    public string StreamName => _streamName;
    public int ActiveRequestCount => _registry.Count;

    public RequestManager(
        IStreamRegistry streamRegistry,
        IRequestRegistry<TRequest, TResponse> registry,
        IRequestIdentityProvider<TRequest> identityProvider,
        IStreamingRequestHandler<TRequest, TResponse> handler,
        ILeaderElectionFactory leaderElectionFactory,
        IStreamingTransport transport,
        ISubjectResolver subjects,
        IOptions<StateSyncOptions> stateSyncOptions,
        IStreamingContextFactory<TRequest, TResponse> contextFactory,
        IConfirmationPublisherFactory confirmationPublisherFactory,
        ILoggerFactory loggerFactory, 
        IMessageSerializer serializer)
    {
        _streamName = streamRegistry.GetStreamName<TRequest>();
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _identityProvider = identityProvider ?? throw new ArgumentNullException(nameof(identityProvider));
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        _stateSyncOptions = stateSyncOptions ?? throw new ArgumentNullException(nameof(stateSyncOptions));
        _logger = loggerFactory.CreateLogger<RequestManager<TRequest, TResponse>>();

        _contextFactory = contextFactory ?? throw new ArgumentNullException(nameof(contextFactory));
        _serializer = serializer;
        _confirmationPublisher = confirmationPublisherFactory.Create(_streamName);

        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _subjects = subjects ?? throw new ArgumentNullException(nameof(subjects));
        
        // Get leader election service for this stream
        _leaderElection = leaderElectionFactory.GetOrCreate(_streamName);
        _currentEpoch = _leaderElection.CurrentEpoch;

        // Resolve channel names
        _requestsSubject = subjects.GetRequestsSubject(_streamName);
        _batchSubject = subjects.GetBatchSyncSubject(_streamName);

        // Subscribe to leadership changes
        _leaderElection.LeadershipChanged += OnLeadershipChanged;

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
            
            // Start batch sync loop
            _batchSyncCts = new CancellationTokenSource();
            _batchSyncTask = RunBatchSyncLoopAsync(_batchSyncCts.Token);

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

                return Task.FromResult(requestId);
            }

            // Create new metadata
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
                Epoch = _currentEpoch,
                LatestImage = default
            };

            // Add to registry
            if (_registry.TryAdd(requestId, metadata))
            {
                _logger.LogInformation(
                    "Opened new {Behavior} request '{RequestId}' for stream '{StreamName}' (total: {Count})",
                    envelope.Behavior,
                    requestId,
                    _streamName,
                    _registry.Count);

                _registry.TryUpdate(requestId, m => m.State = RequestState.Streaming);

                // Invoke handler (runs indefinitely for Live streams)
                // Fire-and-forget: don't await - handler runs in background
                _ = InvokeHandlerOpenedAsync(
                    envelope.Request,
                    requestId,
                    envelope.Behavior,
                    cancellationToken);
            }
            else
            {
                // Race condition: another thread added it
                _registry.TryUpdate(requestId, m => m.SubscriberCount++);
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
            if (!_registry.TryGet(requestId, out var metadata))
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
            if (closeEvent.Epoch < _currentEpoch)
            {
                _logger.LogWarning(
                    "Ignoring stale close event for request '{RequestId}' (epoch {EventEpoch} < {CurrentEpoch})",
                    closeEvent.RequestId,
                    closeEvent.Epoch,
                    _currentEpoch);
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

            _logger.LogDebug(
                "Publishing batch sync for stream '{StreamName}' with {Count} requests",
                _streamName,
                allRequests.Count);

            var batch = new BatchSyncMessage
            {
                Epoch = _currentEpoch,
                Timestamp = DateTime.UtcNow,
                StreamName = _streamName,
                ActiveRequests = allRequests.Select(m => new ActiveRequestSnapshot
                {
                    RequestId = m.RequestId,
                    SerializedRequest = m.SerializedRequest,
                    StreamBehavior = m.StreamBehavior,     // ← ADDED
                    State = m.State,
                    SubscriberCount = m.SubscriberCount,
                    OpenedAt = m.OpenedAt,
                    LastUpdateAt = m.LastUpdateAt
                }).ToList()
            };

            var data = _serializer.Serialize(batch);
            var subscriberCount = await _transport.PublishAsync(_batchSubject, data, cancellationToken);

            _logger.LogTrace(
                "Published batch sync for stream '{StreamName}' to {SubscriberCount} subscribers",
                _streamName,
                subscriberCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error publishing batch sync for stream '{StreamName}'",
                _streamName);
            throw;
        }
    }

    #endregion

    #region Batch Sync - Receiving (All Instances)

    private async Task OnBatchSyncReceivedAsync(byte[] data)
    {
        try
        {
            var batch = _serializer.Deserialize<BatchSyncMessage>(data);

            if (batch.StreamName != _streamName)
            {
                _logger.LogWarning(
                    "Received batch sync for wrong stream. Expected: '{Expected}', Actual: '{Actual}'",
                    _streamName,
                    batch.StreamName);
                return;
            }

            _logger.LogDebug(
                "Processing batch sync for stream '{StreamName}' with {Count} requests (epoch {Epoch})",
                _streamName,
                batch.ActiveRequests.Count,
                batch.Epoch);

            // Validate epoch
            if (batch.Epoch < _currentEpoch)
            {
                _logger.LogWarning(
                    "Ignoring stale batch sync (epoch {BatchEpoch} < current {CurrentEpoch})",
                    batch.Epoch,
                    _currentEpoch);
                return;
            }

            _currentEpoch = batch.Epoch;

            // Reconcile each request
            foreach (var snapshot in batch.ActiveRequests)
            {
                await ReconcileRequestAsync(snapshot, CancellationToken.None);
            }

            // Detect orphaned requests
            await DetectOrphanedRequestsAsync(batch.ActiveRequests, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error processing batch sync for stream '{StreamName}'",
                _streamName);
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
                    metadata.Epoch = _currentEpoch;
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
                Epoch = _currentEpoch,
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

    #endregion

    #region Incoming Requests (Redis Channel)

    private async Task OnRequestReceivedAsync(byte[] data)
    {
        try
        {
            // Deserialize envelope (wraps user request + behavior + correlationId)
            var envelope = _serializer.Deserialize<RequestEnvelope<TRequest>>(data);

            _logger.LogDebug(
                "Received {Behavior} request from Redis for stream '{StreamName}'",
                envelope.Behavior,
                _streamName);

            // Open request (idempotent - same request = same RequestId)
            // ALL instances do this (for failover readiness)
            var requestId = await OpenRequestAsync(envelope, CancellationToken.None);

            // ONLY LEADER sends confirmation back to subscriber
            // Subscriber needs the RequestId to filter responses
            await _confirmationPublisher.ConfirmAsync(
                envelope.CorrelationId,
                requestId,
                CancellationToken.None);
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
        _currentEpoch = e.Epoch;

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

        _logger.LogDebug(
            "RequestManager disposed for stream '{StreamName}'",
            _streamName);
    }

    #endregion
}