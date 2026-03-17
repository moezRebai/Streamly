// FILE: Streamly.Core.Runtime/Publishing/ResponsePublisher.cs
// CHANGE: PublishToTransportAsync — distinguish OperationCanceledException
//         (expected on stream close) from real transport errors.
//
// Before: all exceptions logged at ERR and rethrown, flooding logs with
//         hundreds of "Failed to publish response" errors during IterationCleanup
//         when 2000 handler loops are cancelled simultaneously.
//
// After:  OperationCanceledException → Debug (expected shutdown path, not an error)
//         All other exceptions       → Error (real transport failure, rethrown)

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;
using Streamly.Core.Models;
using Streamly.Core.Runtime.Leadership;
using Streamly.Core.Runtime.Registration;
using Streamly.Core.Runtime.RequestManagement;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Core.Runtime.Publishing;

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
    IResponseChangeDetector<TResponse> changeDetector,
    IStreamingTransport transport,
    IMessageSerializer serializer,
    ISubjectResolver subjects,
    IOptions<StreamlyRuntimeOptions> runtimeOptions,
    ILogger<ResponsePublisher<TRequest, TResponse>> logger)
    : IResponsePublisher<TRequest, TResponse>
{
    private readonly ILeaderElectionFactory _leaderElectionFactory = leaderElectionFactory
        ?? throw new ArgumentNullException(nameof(leaderElectionFactory));
    private ILeaderElectionService _leaderElection = null!;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;
    private readonly IRequestRegistry<TRequest, TResponse> _registry = registry
        ?? throw new ArgumentNullException(nameof(registry));
    private readonly IResponseChangeDetector<TResponse> _changeDetector = changeDetector
        ?? throw new ArgumentNullException(nameof(changeDetector));
    private readonly IStreamingTransport _transport = transport
        ?? throw new ArgumentNullException(nameof(transport));
    private readonly IMessageSerializer _serializer = serializer
        ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ISubjectResolver _subjects = subjects
        ?? throw new ArgumentNullException(nameof(subjects));
    private readonly ILogger<ResponsePublisher<TRequest, TResponse>> _logger = logger
        ?? throw new ArgumentNullException(nameof(logger));

    private readonly string _streamName = streamRegistry.GetStreamName<TRequest>();
    private readonly string _instanceId = runtimeOptions.Value.InstanceId
        ?? throw new ArgumentNullException(nameof(runtimeOptions));

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

        if (closeReason == null)
        {
            var changedProperties = _changeDetector.GetChangedProperties(
                metadata.LatestImage, response);

            if (changedProperties.Count == 0)
            {
                _logger.LogTrace(
                    "Skipping publish for request '{RequestId}' - no changed properties",
                    requestId);
                return;
            }

            _logger.LogTrace(
                "Request '{RequestId}' has {Count} changed properties: {Properties}",
                requestId, changedProperties.Count, string.Join(", ", changedProperties));
        }

        var published = false;
        _registry.TryUpdate(requestId, m =>
        {
            if (!_leaderElection.IsLeader) return;
            m.LatestImage = response;
            m.LastUpdateAt = DateTime.UtcNow;
            if (closeReason.HasValue)
                m.State = RequestState.Closing;
            published = true;
        });

        if (!published)
        {
            _logger.LogDebug(
                "Publish cancelled for request '{RequestId}' - lost leadership during update",
                requestId);
            return;
        }

        var message = new InternalResponseMessage<TResponse>
        {
            RequestId  = requestId,
            Data       = response,
            Epoch      = _leaderElection.CurrentEpoch,
            PublisherId = _instanceId,
            Timestamp  = DateTime.UtcNow,
            IsFinal    = closeReason.HasValue,
            CloseReason = closeReason
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
            // Expected when the handler's CancellationToken is cancelled by
            // CloseRequestAsync (stream closed normally or benchmark cleanup).
            // Not an error — log at Debug to keep logs clean during burst teardown.
            _logger.LogDebug(
                "Publish cancelled for request '{RequestId}' — stream closing",
                requestId);

            // Do not rethrow — the handler loop will exit naturally on the next
            // iteration when it checks cancellationToken.IsCancellationRequested.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to publish response for request '{RequestId}'", requestId);
            throw;
        }
    }
}