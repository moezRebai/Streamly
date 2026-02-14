using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Models;
using Streamly.Core.Runtime.Channel;
using Streamly.Infrastructure.Interfaces;
using Streamly.Subscriber.Configuration;

namespace Streamly.Subscriber.Internal;

/// <summary>
/// Core of the subscriber side
///
/// Responsibilities:
/// 1. Maintain ONE Redis subscription per stream type (efficient)
/// 2. Route incoming responses to correct observer by RequestId
/// 3. Handle confirmation handshake (CorrelationId → RequestId)
/// 4. Manage subscription lifecycle (add, remove, auto-close)
/// 5. Parallel dispatch via worker pool (handles 10,000 simultaneous updates)
/// 6. Epoch validation (reject stale messages)
///
/// ONE instance per stream type (TRequest/TResponse pair)
/// </summary>
internal class SubscriptionManager<TRequest, TResponse> : IAsyncDisposable
{
    private readonly IRedisConnectionManager _redis;
    private readonly IMessageSerializer _serializer;
    private readonly IChannelNameResolver _channelResolver;
    private readonly SubscriberOptions _options;
    private readonly ILogger<SubscriptionManager<TRequest, TResponse>> _logger;

    private readonly string _streamName;
    private readonly string _responsesChannel;
    private readonly string _confirmChannel;

    // RequestId → SubscriptionState (multiple observers per RequestId possible)
    private readonly ConcurrentDictionary<string, List<SubscriptionState<TResponse>>> _byRequestId = new();

    // CorrelationId → SubscriptionState (during handshake, before RequestId known)
    private readonly ConcurrentDictionary<string, SubscriptionState<TResponse>> _byCorrelationId = new();

    // Worker pool for parallel dispatch
    private DispatchWorkerPool<TResponse>? _workerPool;

    // Track if we have active Redis subscriptions
    private int _activeSubscriptionCount;
    private bool _redisSubscribed;
    private readonly SemaphoreSlim _subscriptionLock = new(1, 1);
    private bool _disposed;

    public SubscriptionManager(
        string streamName,
        IRedisConnectionManager redis,
        IMessageSerializer serializer,
        IChannelNameResolver channelResolver,
        IOptions<SubscriberOptions> options,
        ILogger<SubscriptionManager<TRequest, TResponse>> logger)
    {
        _streamName = streamName;
        _redis = redis ?? throw new ArgumentNullException(nameof(redis));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _channelResolver = channelResolver ?? throw new ArgumentNullException(nameof(channelResolver));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _responsesChannel = _channelResolver.GetResponsesChannel(_streamName);
        _confirmChannel = _channelResolver.GetConfirmChannel(_streamName);
    }

    /// <summary>
    /// Register a new subscription
    /// Called before publishing RequestEnvelope to Redis
    /// Returns CorrelationId to use in the envelope
    /// </summary>
    public async Task RegisterPendingAsync(SubscriptionState<TResponse> state,
        CancellationToken cancellationToken)
    {
        await EnsureRedisSubscribedAsync(cancellationToken);

        // Register by CorrelationId (pending confirmation)
        _byCorrelationId[state.CorrelationId] = state;

        Interlocked.Increment(ref _activeSubscriptionCount);

        _logger.LogDebug(
            "Registered pending subscription with correlationId '{CorrelationId}' for stream '{StreamName}'",
            state.CorrelationId,
            _streamName);
    }

    /// <summary>
    /// Called when client disposes subscription
    /// Removes observer, sends unsubscribe if last observer for RequestId
    /// </summary>
    public async Task UnregisterAsync(
        SubscriptionState<TResponse> state,
        CancellationToken cancellationToken = default)
    {
        if (state.IsDisposed) return;
        state.IsDisposed = true;

        _logger.LogDebug(
            "Unregistering subscription for RequestId '{RequestId}'",
            state.RequestId ?? state.CorrelationId);

        // Remove from pending if still waiting for confirmation
        if (state.WaitingForConfirmation)
        {
            _byCorrelationId.TryRemove(state.CorrelationId, out _);
        }

        // Remove from active subscriptions
        if (state.RequestId != null)
        {
            RemoveFromRequestIdDict(state);

            // If no more observers for this RequestId, send unsubscribe signal
            if (!_byRequestId.ContainsKey(state.RequestId) ||
                _byRequestId[state.RequestId].Count == 0)
            {
                await SendUnsubscribeAsync(state.RequestId, cancellationToken);
            }
        }

        Interlocked.Decrement(ref _activeSubscriptionCount);

        // If no more active subscriptions, unsubscribe from Redis
        if (_activeSubscriptionCount <= 0)
        {
            await UnsubscribeFromRedisAsync();
        }
    }

    #region Redis Message Handlers

    /// <summary>
    /// Called when leader sends confirmation
    /// Moves subscription from CorrelationId dict to RequestId dict
    /// </summary>
    private Task OnConfirmationReceivedAsync(byte[] data)
    {
        try
        {
            var confirmation = _serializer.Deserialize<ConfirmationMessage>(data);

            if (confirmation.StreamName != _streamName)
                return Task.CompletedTask;

            _logger.LogDebug(
                "Received confirmation: correlationId '{CorrelationId}' → requestId '{RequestId}'",
                confirmation.CorrelationId,
                confirmation.RequestId);

            // Find pending subscription by CorrelationId
            if (!_byCorrelationId.TryRemove(confirmation.CorrelationId, out var state))
            {
                _logger.LogDebug(
                    "No pending subscription for correlationId '{CorrelationId}', ignoring",
                    confirmation.CorrelationId);
                return Task.CompletedTask;
            }

            // Promote: now we know the real RequestId
            state.RequestId = confirmation.RequestId;
            state.LastKnownEpoch = confirmation.Epoch;
            state.WaitingForConfirmation = false;

            // Add to RequestId routing dict
            _byRequestId.AddOrUpdate(
                confirmation.RequestId,
                _ => new List<SubscriptionState<TResponse>> { state },
                (_, existing) =>
                {
                    lock (existing) { existing.Add(state); }
                    return existing;
                });

            _logger.LogInformation(
                "Subscription confirmed for RequestId '{RequestId}' (stream '{StreamName}')",
                confirmation.RequestId,
                _streamName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error processing confirmation for stream '{StreamName}'",
                _streamName);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Called when a response arrives from Redis
    /// Routes to worker pool for parallel processing
    /// </summary>
    private Task OnResponseReceivedAsync(byte[] data)
    {
        try
        {
            // Quick peek at RequestId only (avoid full deserialization here for speed)
            // Full deserialization happens in worker
            var preview = _serializer.Deserialize<ResponsePreview>(data);

            if (!_byRequestId.ContainsKey(preview.RequestId))
            {
                // No subscriber for this RequestId, ignore
                _logger.LogTrace(
                    "No subscriber for RequestId '{RequestId}', ignoring",
                    preview.RequestId);
                return Task.CompletedTask;
            }

            // Dispatch to worker pool (non-blocking)
            _workerPool?.Dispatch(preview.RequestId, data, preview.Epoch);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error routing response for stream '{StreamName}'",
                _streamName);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Called by worker pool to fully process a dispatched message
    /// Full deserialization + epoch validation + observer dispatch
    /// </summary>
    private async Task ProcessDispatchItemAsync(DispatchItem item)
    {
        try
        {
            // Full deserialization in worker
            var message = _serializer.Deserialize<InternalResponseMessage<TResponse>>(item.Data);

            // Get observers for this RequestId
            if (!_byRequestId.TryGetValue(message.RequestId, out var states))
                return;

            List<SubscriptionState<TResponse>> snapshot;
            lock (states) { snapshot = new List<SubscriptionState<TResponse>>(states); }

            foreach (var state in snapshot)
            {
                if (state.IsDisposed) continue;

                // Validate epoch (reject stale messages)
                if (message.Epoch < state.LastKnownEpoch)
                {
                    _logger.LogWarning(
                        "Rejecting stale message for RequestId '{RequestId}' (epoch {MsgEpoch} < {KnownEpoch})",
                        message.RequestId,
                        message.Epoch,
                        state.LastKnownEpoch);
                    continue;
                }

                state.LastKnownEpoch = message.Epoch;

                // Deliver response to observer (epoch stripped - user never sees it)
                state.Subject.OnNext(message.Data);

                _logger.LogTrace(
                    "Dispatched response for RequestId '{RequestId}' to observer",
                    message.RequestId);

                // Handle stream closure
                if (message.IsFinal)
                {
                    await HandleFinalMessageAsync(state, message.CloseReason ?? CloseReason.Normal);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error processing dispatch item for RequestId '{RequestId}'",
                item.RequestId);
        }
    }

    private async Task HandleFinalMessageAsync(
        SubscriptionState<TResponse> state,
        CloseReason reason)
    {
        _logger.LogInformation(
            "Stream closing for RequestId '{RequestId}', reason: {Reason}",
            state.RequestId,
            reason);

        // Determine if should reconnect based on reason
        var shouldReconnect = reason switch
        {
            CloseReason.Normal => false,
            CloseReason.Unsubscribe => false,
            CloseReason.Error => true,
            CloseReason.Timeout => true,
            CloseReason.Orphaned => true,
            CloseReason.Shutdown => true,
            _ => false
        };

        if (shouldReconnect && state.ReconnectAttempts < _options.MaxReconnectAttempts)
        {
            _logger.LogInformation(
                "Reconnecting subscription for RequestId '{RequestId}' (attempt {Attempt}/{Max})",
                state.RequestId,
                state.ReconnectAttempts + 1,
                _options.MaxReconnectAttempts);

            // TODO: Trigger Polly reconnect (will implement in StreamingSubscriber)
            // For now signal the subscriber to handle reconnect
            state.ReconnectAttempts++;
        }
        else
        {
            // Complete the observable (no reconnect)
            state.Subject.OnCompleted();
            await UnregisterAsync(state);
        }
    }

    #endregion

    #region Redis Subscription Management

    private async Task EnsureRedisSubscribedAsync(CancellationToken cancellationToken)
    {
        if (_redisSubscribed) return;

        await _subscriptionLock.WaitAsync(cancellationToken);
        try
        {
            if (_redisSubscribed) return;

            _logger.LogInformation(
                "Creating Redis subscriptions for stream '{StreamName}'",
                _streamName);

            // Subscribe to confirmation channel
            await _redis.SubscribeAsync(_confirmChannel, OnConfirmationReceivedAsync);

            // Subscribe to responses channel
            await _redis.SubscribeAsync(_responsesChannel, OnResponseReceivedAsync);

            // Start worker pool
            _workerPool = new DispatchWorkerPool<TResponse>(
                _options.DispatchWorkerCount,
                _options.DispatchChannelCapacity,
                ProcessDispatchItemAsync,
                _logger);

            _redisSubscribed = true;

            _logger.LogInformation(
                "Redis subscriptions created for stream '{StreamName}' " +
                "({WorkerCount} dispatch workers)",
                _streamName,
                _options.DispatchWorkerCount);
        }
        finally
        {
            _subscriptionLock.Release();
        }
    }

    private async Task UnsubscribeFromRedisAsync()
    {
        await _subscriptionLock.WaitAsync();
        try
        {
            if (!_redisSubscribed) return;

            _logger.LogInformation(
                "Removing Redis subscriptions for stream '{StreamName}' (no active subscribers)",
                _streamName);

            await _redis.UnsubscribeAsync(_confirmChannel);
            await _redis.UnsubscribeAsync(_responsesChannel);

            if (_workerPool != null)
            {
                await _workerPool.DisposeAsync();
                _workerPool = null;
            }

            _redisSubscribed = false;

            _logger.LogInformation(
                "Redis subscriptions removed for stream '{StreamName}'",
                _streamName);
        }
        finally
        {
            _subscriptionLock.Release();
        }
    }

    #endregion

    #region Helpers

    private void RemoveFromRequestIdDict(SubscriptionState<TResponse> state)
    {
        if (state.RequestId == null) return;

        if (_byRequestId.TryGetValue(state.RequestId, out var states))
        {
            lock (states)
            {
                states.Remove(state);
                if (states.Count == 0)
                    _byRequestId.TryRemove(state.RequestId, out _);
            }
        }
    }

    private async Task SendUnsubscribeAsync(string requestId, CancellationToken cancellationToken)
    {
        try
        {
            var unsubscribeChannel = _channelResolver.GetUnsubscribeChannel(_streamName);

            var envelope = new UnsubscribeEnvelope
            {
                RequestId = requestId,
                StreamName = _streamName,
                UnsubscribedAt = DateTime.UtcNow
            };

            var data = _serializer.Serialize(envelope);
            await _redis.PublishAsync(unsubscribeChannel, data, cancellationToken);

            _logger.LogInformation(
                "Sent unsubscribe signal for RequestId '{RequestId}'",
                requestId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to send unsubscribe signal for RequestId '{RequestId}'",
                requestId);
        }
    }

    #endregion

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Complete all active subjects
        foreach (var states in _byRequestId.Values)
            lock (states)
                foreach (var s in states)
                    s.Subject.OnCompleted();

        foreach (var state in _byCorrelationId.Values)
            state.Subject.OnCompleted();

        await UnsubscribeFromRedisAsync();

        _subscriptionLock.Dispose();

        _logger.LogInformation(
            "SubscriptionManager disposed for stream '{StreamName}'",
            _streamName);
    }
}

/// <summary>
/// Lightweight preview for quick RequestId extraction
/// Avoids full deserialization in the Redis callback
/// </summary>
internal class ResponsePreview
{
    [System.Text.Json.Serialization.JsonPropertyName("requestId")]
    public string RequestId { get; set; } = string.Empty;

    [System.Text.Json.Serialization.JsonPropertyName("epoch")]
    public long Epoch { get; set; }

    [System.Text.Json.Serialization.JsonPropertyName("isFinal")]
    public bool IsFinal { get; set; }
}