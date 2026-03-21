using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.ChangeDetection;
using Streamly.Core.Configurations;
using Streamly.Core.Models;
using Streamly.Server.Leadership;
using Streamly.Server.Registration;
using Streamly.Server.RequestManagement;
using Streamly.Infrastructure.Interfaces;
using Streamly.Server.Publishing.Interfaces;

namespace Streamly.Server.Publishing;

/// <summary>
/// Orchestrates the response publishing pipeline:
/// 1. Check leadership
/// 2. Compare with latest image
/// 3. Update latest image
/// 4. Publish to transport
/// </summary>
internal class ResponsePublisher<TRequest, TResponse>(
    IStreamRegistry streamRegistry,
    ILeaderElectionFactory leaderElectionFactory,
    IRequestRegistry<TRequest, TResponse> registry,
    IResponseDiffComputer<TResponse> diffComputer,
    IStreamingTransport transport,
    IMessageSerializer serializer,
    ISubjectResolver subjects,
    IOptions<StreamlySettings> runtimeOptions,
    ILogger<ResponsePublisher<TRequest, TResponse>> logger)
    : IResponsePublisher<TRequest, TResponse>, IAsyncDisposable
    where TResponse : class
{
    private readonly ILeaderElectionFactory _leaderElectionFactory = leaderElectionFactory
        ?? throw new ArgumentNullException(nameof(leaderElectionFactory));
    private ILeaderElectionService _leaderElection = null!;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    // Fix #2: volatile required for safe double-checked locking in EnsureInitializedAsync
    private volatile bool _initialized;
    private readonly IRequestRegistry<TRequest, TResponse> _registry = registry
        ?? throw new ArgumentNullException(nameof(registry));
    private readonly IResponseDiffComputer<TResponse> _diffComputer = diffComputer
        ?? throw new ArgumentNullException(nameof(diffComputer));
    private readonly IStreamingTransport _transport = transport
        ?? throw new ArgumentNullException(nameof(transport));
    private readonly IMessageSerializer _serializer = serializer
        ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ISubjectResolver _subjects = subjects
        ?? throw new ArgumentNullException(nameof(subjects));
    private readonly ILogger<ResponsePublisher<TRequest, TResponse>> _logger = logger
        ?? throw new ArgumentNullException(nameof(logger));

    // Fix #4: explicit null guards — field initializers throw NullReferenceException otherwise
    private readonly string _streamName =
        (streamRegistry ?? throw new ArgumentNullException(nameof(streamRegistry)))
        .GetStreamName<TRequest>();
    private readonly string _instanceId =
        (runtimeOptions ?? throw new ArgumentNullException(nameof(runtimeOptions)))
        .Value.InstanceId;

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized) return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_initialized) return;
            _leaderElection = await _leaderElectionFactory.GetOrCreateAsync(
                _streamName, cancellationToken);
            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    public async Task PublishAsync(
        string requestId,
        TResponse response,
        CloseReason? closeReason = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(requestId))
            throw new ArgumentException("Request ID cannot be null or whitespace", nameof(requestId));
        ArgumentNullException.ThrowIfNull(response);

        await EnsureInitializedAsync(cancellationToken);

        if (!_leaderElection.IsLeader)
        {
            if (_registry.TryGet(requestId, out var followerMeta)
                && followerMeta?.State == RequestState.Streaming)
            {
                _registry.TryUpdate(requestId, m =>
                {
                    m.LatestImage = response;
                    m.LastUpdateAt = DateTime.UtcNow;
                });
            }

            _logger.LogTrace(
                "Skipping publish for request '{RequestId}' - not leader", requestId);
            return;
        }

        if (!_registry.TryGet(requestId, out var metadata) || metadata == null)
        {
            _logger.LogWarning(
                "Cannot publish for request '{RequestId}' - not found in registry", requestId);
            return;
        }

        if (metadata.State != RequestState.Streaming)
        {
            _logger.LogDebug(
                "Skipping publish for request '{RequestId}' - state is {State}",
                requestId, metadata.State);
            return;
        }

        // For normal updates compute the diff; skip publish entirely if nothing changed.
        // For close messages skip diffing — always send the full final image as a snapshot.
        ResponseDiff<TResponse>? diff = null;
        if (closeReason == null)
        {
            diff = _diffComputer.Compute(metadata.LatestImage, response);

            if (diff.Update == null)
            {
                _logger.LogTrace(
                    "Skipping publish for request '{RequestId}' - no changed properties",
                    requestId);
                return;
            }

            if (diff.ChangedProperties != null)
                _logger.LogTrace(
                    "Request '{RequestId}' has {Count} changed properties: {Properties}",
                    requestId, diff.ChangedProperties.Count,
                    string.Join(", ", diff.ChangedProperties));
            else
                _logger.LogTrace(
                    "Publishing full snapshot for request '{RequestId}' (first message)",
                    requestId);
        }

        // leaderDuringUpdate here is safe — no retry loop that could produce a false positive.
        var leaderDuringUpdate = false;
        _registry.TryUpdate(requestId, m =>
        {
            if (!_leaderElection.IsLeader) return;
            m.LatestImage = diff?.LatestImage ?? response;
            m.LastUpdateAt = DateTime.UtcNow;
            if (closeReason.HasValue)
                m.State = RequestState.Closing;
            leaderDuringUpdate = true;
        });

        if (!leaderDuringUpdate)
        {
            _logger.LogDebug(
                "Publish cancelled for request '{RequestId}' - lost leadership during update",
                requestId);
            return;
        }

        var message = new InternalResponseMessage<TResponse>
        {
            RequestId         = requestId,
            Data              = closeReason.HasValue ? response : diff!.Update!,
            ChangedProperties = closeReason.HasValue ? null : diff!.ChangedProperties,
            Epoch             = _leaderElection.CurrentEpoch,
            PublisherId       = _instanceId,
            Timestamp         = DateTime.UtcNow,
            IsFinal           = closeReason.HasValue,
            CloseReason       = closeReason
        };

        await PublishToTransportAsync(message, requestId, cancellationToken);

        if (closeReason.HasValue)
            await PublishCloseEventAsync(requestId, closeReason.Value, cancellationToken);
    }

    public async Task CloseAsync(
        string requestId,
        CloseReason reason = CloseReason.Normal,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(requestId))
            throw new ArgumentException("Request ID cannot be null or whitespace", nameof(requestId));

        await EnsureInitializedAsync(cancellationToken);

        if (!_leaderElection.IsLeader)
        {
            _logger.LogTrace(
                "Skipping close for request '{RequestId}' - not leader", requestId);
            return;
        }

        if (!_registry.TryGet(requestId, out _))
        {
            _logger.LogWarning(
                "Cannot close request '{RequestId}' - not found in registry", requestId);
            return;
        }

        _logger.LogInformation(
            "Closing request '{RequestId}' with reason: {Reason} (no final response)",
            requestId, reason);

        _registry.TryUpdate(requestId, m => m.State = RequestState.Closing);

        await PublishCloseEventAsync(requestId, reason, cancellationToken);
    }

    private async Task PublishCloseEventAsync(
        string requestId,
        CloseReason reason,
        CancellationToken cancellationToken)
    {
        try
        {
            var closeEvent = new RequestClosedEvent
            {
                RequestId  = requestId,
                StreamName = _streamName,
                Reason     = reason,
                Epoch      = _leaderElection.CurrentEpoch,
                Timestamp  = DateTime.UtcNow
            };

            var eventsSubject = _subjects.GetCloseEventsSubject(_streamName);
            var eventData     = _serializer.Serialize(closeEvent);

            await _transport.PublishAsync(eventsSubject, eventData, cancellationToken);

            _logger.LogInformation(
                "Published close event for request '{RequestId}' (reason: {Reason})",
                requestId, reason);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to publish close event for request '{RequestId}'", requestId);
        }
    }

    public async Task ForcePublishLatestImageAsync(
        string requestId,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        if (!_leaderElection.IsLeader) return;

        if (!_registry.TryGet(requestId, out var metadata) || metadata == null) return;

        if (metadata.State != RequestState.Streaming || metadata.LatestImage == null) return;

        var message = new InternalResponseMessage<TResponse>
        {
            RequestId   = requestId,
            Data        = metadata.LatestImage,
            Epoch       = _leaderElection.CurrentEpoch,
            PublisherId = _instanceId,
            Timestamp   = DateTime.UtcNow,
            IsFinal     = false,
            CloseReason = null
        };

        _logger.LogDebug(
            "Force-publishing latest image for request '{RequestId}' after leadership gain",
            requestId);

        await PublishToTransportAsync(message, requestId, cancellationToken);
    }

    private async Task PublishToTransportAsync(
        InternalResponseMessage<TResponse> message,
        string requestId,
        CancellationToken cancellationToken)
    {
        try
        {
            var data = _serializer.Serialize(message);
            var responsesSubject = _subjects.GetResponsesSubject(_streamName, requestId);

            var subscriberCount = await _transport.PublishAsync(
                responsesSubject, data, cancellationToken);

            _logger.LogTrace(
                "Published response for request '{RequestId}' " +
                "(epoch {Epoch}, final: {IsFinal}) to {SubscriberCount} subscribers",
                message.RequestId, message.Epoch, message.IsFinal, subscriberCount);
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug(
                "Publish cancelled for request '{RequestId}' — stream closing",
                requestId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to publish response for request '{RequestId}'", requestId);
            throw;
        }
    }

    // Fix #1: dispose _initLock to avoid SemaphoreSlim resource leak
    public ValueTask DisposeAsync()
    {
        _initLock.Dispose();
        return ValueTask.CompletedTask;
    }
}
