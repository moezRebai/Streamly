using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Models;
using Streamly.Core.Runtime.Configuration;
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
/// 
/// </summary>
internal class ResponsePublisher<TRequest, TResponse> : IResponsePublisher<TRequest, TResponse>
{
    private readonly ILeaderElectionService _leaderElection;
    private readonly IRequestRegistry<TRequest, TResponse> _registry;
    private readonly IResponseChangeDetector<TResponse> _changeDetector;
    private readonly IStreamingTransport _transport;
    private readonly IMessageSerializer _serializer;
    private readonly ISubjectResolver _subjects;
    private readonly ILogger<ResponsePublisher<TRequest, TResponse>> _logger;

    private readonly string _streamName;
    private readonly string _instanceId;

    public ResponsePublisher(
        IStreamRegistry streamRegistry,
        ILeaderElectionFactory leaderElectionFactory,
        IRequestRegistry<TRequest, TResponse> registry,
        IResponseChangeDetector<TResponse> changeDetector,
        IStreamingTransport transport,
        IMessageSerializer serializer,
        ISubjectResolver subjects,
        IOptions<StreamlyRuntimeOptions> runtimeOptions,
        ILogger<ResponsePublisher<TRequest, TResponse>> logger)
    {
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _changeDetector = changeDetector ?? throw new ArgumentNullException(nameof(changeDetector));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _subjects = subjects ?? throw new ArgumentNullException(nameof(subjects));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _instanceId = runtimeOptions.Value.InstanceId ?? throw new ArgumentNullException(nameof(runtimeOptions));

        // Get stream name from registry
        _streamName = streamRegistry.GetStreamName<TRequest>();

        // Get leader election service for this stream
        _leaderElection = leaderElectionFactory.GetOrCreate(_streamName);
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

        // Step 1: Check leadership
        if (!_leaderElection.IsLeader)
        {
            _logger.LogTrace(
                "Skipping publish for request '{RequestId}' - not leader",
                requestId);
            return;
        }

        // Step 2: Get registry entry
        if (!_registry.TryGet(requestId, out var metadata) || metadata == null)
        {
            _logger.LogWarning(
                "Cannot publish for request '{RequestId}' - not found in registry",
                requestId);
            return;
        }

        // Step 3: Validate state
        if (metadata.State != RequestState.Streaming)
        {
            _logger.LogDebug(
                "Skipping publish for request '{RequestId}' - state is {State}",
                requestId,
                metadata.State);
            return;
        }

        // Step 4: Detect changes (skip for final publish)
        if (closeReason == null)
        {
            var changedProperties = _changeDetector.GetChangedProperties(
                metadata.LatestImage, 
                response);

            if (changedProperties.Count == 0)
            {
                _logger.LogTrace(
                    "Skipping publish for request '{RequestId}' - no changed properties",
                    requestId);
                return;
            }

            _logger.LogTrace(
                "Request '{RequestId}' has {Count} changed properties: {Properties}",
                requestId,
                changedProperties.Count,
                string.Join(", ", changedProperties));
        }

        // Step 5: Update latest image atomically
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

        // Step 6: Build and publish internal message
        var message = new InternalResponseMessage<TResponse>
        {
            RequestId = requestId,
            Data = response,
            Epoch = _leaderElection.CurrentEpoch,
            PublisherId = _instanceId,
            Timestamp = DateTime.UtcNow,
            IsFinal = closeReason.HasValue,
            CloseReason = closeReason
        };

        await PublishToTransportAsync(message, requestId, cancellationToken);

        // Step 7: If closing, broadcast close event to service instances
        if (closeReason.HasValue)
        {
            await PublishCloseEventAsync(requestId, closeReason.Value, cancellationToken);
        }
    }

    public async Task CloseAsync(
        string requestId,
        CloseReason reason = CloseReason.Normal,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(requestId))
            throw new ArgumentException("Request ID cannot be null or whitespace", nameof(requestId));

        // Check leadership
        if (!_leaderElection.IsLeader)
        {
            _logger.LogTrace(
                "Skipping close for request '{RequestId}' - not leader",
                requestId);
            return;
        }

        // Verify request exists
        if (!_registry.TryGet(requestId, out _))
        {
            _logger.LogWarning(
                "Cannot close request '{RequestId}' - not found in registry",
                requestId);
            return;
        }

        _logger.LogInformation(
            "Closing request '{RequestId}' with reason: {Reason} (no final response)",
            requestId,
            reason);

        // Update state
        _registry.TryUpdate(requestId, m => m.State = RequestState.Closing);

        // Broadcast close event to service instances and clients
        await PublishCloseEventAsync(requestId, reason, cancellationToken);
    }

    private async Task PublishCloseEventAsync(
        string requestId,
        CloseReason reason,
        CancellationToken cancellationToken)
    {
        try
        {
            // Notify SERVICE INSTANCES via events channel
            var closeEvent = new RequestClosedEvent
            {
                RequestId = requestId,
                StreamName = _streamName,
                Reason = reason,
                Epoch = _leaderElection.CurrentEpoch,
                Timestamp = DateTime.UtcNow
            };

            var eventsSubject = _subjects.GetCloseEventsSubject(_streamName);
            var eventData = _serializer.Serialize(closeEvent);

            await _transport.PublishAsync(eventsSubject, eventData, cancellationToken);

            _logger.LogInformation(
                "Published close event for request '{RequestId}' " +
                "to events subject (reason: {Reason})",
                requestId,
                reason);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to publish close event for request '{RequestId}'",
                requestId);
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

            // IMPORTANT: Pass requestId for NATS per-request subjects
            // Redis: returns "streams.responses.{streamName}" (single channel)
            // NATS:  returns "streams.responses.{streamName}.{requestId}" (per-request subject)
            var responsesSubject = _subjects.GetResponsesSubject(_streamName, requestId);

            var subscriberCount = await _transport.PublishAsync(
                responsesSubject,
                data,
                cancellationToken);

            _logger.LogTrace(
                "Published response for request '{RequestId}' " +
                "(epoch {Epoch}, final: {IsFinal}) " +
                "to {SubscriberCount} subscribers",
                message.RequestId,
                message.Epoch,
                message.IsFinal,
                subscriberCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to publish response for request '{RequestId}'",
                message.RequestId);
            throw;
        }
    }
}