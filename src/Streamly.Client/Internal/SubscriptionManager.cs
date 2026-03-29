using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;
using Streamly.Core.Models;
using Streamly.Infrastructure.Interfaces;
using Streamly.Client.Configuration;
using Streamly.Client.Models;

namespace Streamly.Client.Internal;

internal class SubscriptionManager<TRequest, TResponse>(
    string streamName,
    IStreamingTransport transport,
    ISubjectResolver subjects,
    IMessageSerializer serializer,
    IResponseDiffComputer<TResponse> diffComputer,
    IOptions<StreamlySettings> options,
    IStreamlyMetricsCollector metrics,
    ILogger<SubscriptionManager<TRequest, TResponse>> logger)
    : IAsyncDisposable
    where TResponse : class
{
    private readonly IStreamingTransport _transport = transport ?? throw new ArgumentNullException(nameof(transport));
    private readonly ISubjectResolver _subjects = subjects ?? throw new ArgumentNullException(nameof(subjects));
    private readonly IMessageSerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly IResponseDiffComputer<TResponse> _diffComputer = diffComputer ?? throw new ArgumentNullException(nameof(diffComputer));
    private readonly StreamlySettings _settings = options.Value ?? throw new ArgumentNullException(nameof(options));
    private readonly IStreamlyMetricsCollector _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
    private readonly ILogger<SubscriptionManager<TRequest, TResponse>> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly ConcurrentDictionary<string, List<SubscriptionState<TResponse>>> _byRequestId = new();
    private readonly ConcurrentDictionary<string, SubscriptionState<TResponse>> _byCorrelationId = new();
    // Responses that arrived before their confirmation (publisher published faster than confirm round-trip)
    private readonly ConcurrentDictionary<string, ConcurrentQueue<(byte[] Data, long Epoch)>> _pendingResponses = new();

    private ResponseShardDispatcher? _shardDispatcher;
    private int _activeSubscriptionCount;
    private bool _transportSubscribed;
    private readonly SemaphoreSlim _subscriptionLock = new(1, 1);
    private bool _disposed;

    // ── Watchdog ─────────────────────────────────────────────────────────────
    private DateTime _lastResponseTime = DateTime.UtcNow;
    private CancellationTokenSource? _watchdogCts;
    // One CTS per active attempt, keyed by correlationId.
    // The watchdog cancels all of them; each subscription only removes its own.
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _attemptCtsByCorrelationId = new();
    private Task? _watchdogTask;

    // ── Heartbeat ─────────────────────────────────────────────────────────────
    private CancellationTokenSource? _heartbeatCts;
    private Task? _heartbeatTask;

    public async Task RegisterPendingAsync(
        SubscriptionState<TResponse> state,
        CancellationTokenSource attemptCts,
        CancellationToken cancellationToken)
    {
        await EnsureTransportSubscribedAsync(cancellationToken);

        _byCorrelationId[state.CorrelationId] = state;
        Interlocked.Increment(ref _activeSubscriptionCount);

        _attemptCtsByCorrelationId[state.CorrelationId] = attemptCts;

        StartHeartbeat();

        _logger.LogDebug(
            "Registered pending subscription '{CorrelationId}' for stream '{StreamName}'",
            state.CorrelationId, streamName);
    }

    public async Task UnregisterAsync(
        SubscriptionState<TResponse> state,
        bool sendUnsubscribeMessage = true,
        CancellationToken cancellationToken = default)
    {
        if (state.IsDisposed) return;
        state.IsDisposed = true;

        _attemptCtsByCorrelationId.TryRemove(state.CorrelationId, out _);

        if (state.WaitingForConfirmation)
            _byCorrelationId.TryRemove(state.CorrelationId, out _);

        if (state.RequestId != null)
        {
            RemoveFromRequestIdDict(state);

            if (sendUnsubscribeMessage &&
                (!_byRequestId.TryGetValue(state.RequestId, out var value) || value.Count == 0))
            {
                await SendUnsubscribeAsync(state.RequestId, cancellationToken);
            }

            // Record after removal — requestId is available only once confirmed
            _metrics.RecordSubscriptionClosed(state.RequestId, CloseReason.Normal);
        }

        Interlocked.Decrement(ref _activeSubscriptionCount);

        if (_activeSubscriptionCount <= 0)
        {
            StopWatchdog();
            StopHeartbeat();
            await UnsubscribeFromTransportAsync();
        }
    }

    #region Heartbeat

    private void StartHeartbeat()
    {
        if (_heartbeatTask is { IsCompleted: false })
            return; // already running

        _heartbeatCts = new CancellationTokenSource();
        _heartbeatTask = RunHeartbeatAsync(_heartbeatCts.Token);

        _logger.LogDebug(
            "Subscriber heartbeat started for stream '{StreamName}' " +
            "(interval: {Interval}ms)",
            streamName, _settings.Subscriber.HeartbeatIntervalMs);
    }

    private void StopHeartbeat()
    {
        _heartbeatCts?.Cancel();
        _heartbeatCts = null;
    }

    private async Task RunHeartbeatAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_settings.Subscriber.HeartbeatIntervalMs, ct);

                if (_activeSubscriptionCount <= 0) continue;

                var heartbeat = new SubscriberHeartbeatMessage
                {
                    SubscriberId = _settings.InstanceId
                };

                var data = serializer.Serialize(heartbeat);
                var subject = subjects.GetSubscriberHeartbeatSubject();

                await transport.PublishAsync(subject, data, ct);

                _logger.LogTrace(
                    "Subscriber heartbeat sent (subscriberId: '{SubscriberId}')",
                    _settings.InstanceId);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                // Non-fatal — log and keep trying
                _logger.LogWarning(ex,
                    "Failed to send subscriber heartbeat for stream '{StreamName}'",
                    streamName);
            }
        }

        _logger.LogDebug(
            "Subscriber heartbeat stopped for stream '{StreamName}'", streamName);
    }

    #endregion

    #region Watchdog

    private void StartWatchdog()
    {
        if (_watchdogTask is { IsCompleted: false })
            return; // already running

        _lastResponseTime = DateTime.UtcNow; // reset on each new subscription
        _watchdogCts = new CancellationTokenSource();
        _watchdogTask = RunWatchdogAsync(_watchdogCts.Token);

        _logger.LogDebug(
            "Watchdog started for stream '{StreamName}' (timeout: {Timeout}ms)",
            streamName, _settings.Subscriber.HeartbeatTimeoutMs);
    }

    private void StopWatchdog()
    {
        _watchdogCts?.Cancel();
        _watchdogCts = null;
    }

    private async Task RunWatchdogAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(100, ct); // check every 100ms

                if (_activeSubscriptionCount <= 0) continue;

                var elapsed = DateTime.UtcNow - _lastResponseTime;
                if (elapsed.TotalMilliseconds <= _settings.Subscriber.HeartbeatTimeoutMs) continue;

                _logger.LogWarning(
                    "Heartbeat timeout on '{StreamName}': no response for {Elapsed}ms " +
                    "(threshold: {Threshold}ms). Notifying subscribers.",
                    streamName,
                    (int)elapsed.TotalMilliseconds,
                    _settings.Subscriber.HeartbeatTimeoutMs);

                // Record per active requestId — each stream is independently monitored
                foreach (var requestId in _byRequestId.Keys)
                {
                    _metrics.RecordWatchdogTrigger(requestId);
                    _metrics.RecordReconnectionAttempt(requestId);
                }

                // Notify all active subscribers - does NOT kill their observable
                NotifyReconnecting();

                // Reset timer to avoid spamming
                _lastResponseTime = DateTime.UtcNow;
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Watchdog error for stream '{StreamName}'", streamName);
            }
        }
    }

    /// <summary>
    /// Notifies all active states that stream is lost.
    /// Does NOT call OnError/OnCompleted - observable stays alive.
    /// StreamingSubscriber handles retry and calls NotifyRestored when back.
    /// </summary>
    private void NotifyReconnecting()
    {
        var activeStates = _byRequestId.Values
            .SelectMany(list => { lock (list) return list.ToList(); })
            .Concat(_byCorrelationId.Values)
            .Distinct()
            .ToList();

        foreach (var state in activeStates.Where(state => !state.IsDisposed))
        {
            state.ReconnectAttempts++;

            state.NotifyStatus(StreamStatus.Reconnecting(
                streamName,
                state.ReconnectAttempts,
                _settings.Subscriber.MaxReconnectAttempts,
                new PublisherUnavailableException(
                    $"No response from '{streamName}' for {_settings.Subscriber.HeartbeatTimeoutMs}ms")));
        }

        foreach (var cts in _attemptCtsByCorrelationId.Values)
        {
            try { cts.Cancel(); }
            catch (ObjectDisposedException) { }
        }
    }

    /// <summary>
    /// Called by StreamingSubscriber when reconnection succeeds.
    /// Notifies all states that stream is back.
    /// </summary>
    public void NotifyRestored()
    {
        _lastResponseTime = DateTime.UtcNow;

        var activeStates = _byRequestId.Values
            .SelectMany(list => { lock (list) return list.ToList(); })
            .ToList();

        foreach (var state in activeStates)
        {
            if (state.IsDisposed) continue;

            if (state.ReconnectAttempts > 0)
            {
                _logger.LogInformation(
                    "Stream restored for RequestId '{RequestId}' on '{StreamName}'",
                    state.RequestId, streamName);

                state.ReconnectAttempts = 0;
                state.NotifyStatus(StreamStatus.Active(streamName));
            }
        }
    }

    #endregion

    #region Transport Message Handlers

    private Task OnConfirmationReceivedAsync(byte[] data)
    {
        try
        {
            var confirmation = _serializer.Deserialize<ConfirmationMessage>(data);

            if (confirmation.StreamName != streamName)
                return Task.CompletedTask;

            if (!_byCorrelationId.TryRemove(confirmation.CorrelationId, out var state))
                return Task.CompletedTask;

            state.RequestId = confirmation.RequestId;
            state.LastKnownEpoch = confirmation.Epoch;
            state.ConfirmationTcs.TrySetResult();

            _byRequestId.AddOrUpdate(
                confirmation.RequestId,
                _ => [state],
                (_, existing) =>
                {
                    lock (existing) { existing.Add(state); }
                    return existing;
                });

            // Drain any responses that arrived before this confirmation
            DrainPendingResponses(confirmation.RequestId);

            // Start watchdog only after confirmation — the stream is now active.
            StartWatchdog();

            // Subscription is now active — record after watchdog started
            _metrics.RecordSubscriptionOpened(confirmation.RequestId, streamName);

            _logger.LogDebug(
                "Confirmation mapped: correlationId '{CorrelationId}' → requestId '{RequestId}'",
                confirmation.CorrelationId, confirmation.RequestId);

            _logger.LogInformation(
                "Subscription confirmed [{RequestId}] — {@Request}",
                confirmation.RequestId, state.Request);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing confirmation for '{StreamName}'", streamName);
        }

        return Task.CompletedTask;
    }

    private Task OnKeepaliveReceivedAsync(byte[] data)
    {
        // Reset watchdog — publisher is alive even if no prices
        _lastResponseTime = DateTime.UtcNow;

        _logger.LogTrace(
            "Keepalive received for stream '{StreamName}'", streamName);

        return Task.CompletedTask;
    }

    private Task OnResponseReceivedAsync(byte[] data)
    {
        try
        {
            // Reset watchdog on every message received
            _lastResponseTime = DateTime.UtcNow;

            var preview = _serializer.Deserialize<ResponsePreview>(data);

            if (_byRequestId.ContainsKey(preview.RequestId))
            {
                _shardDispatcher?.Dispatch(preview.RequestId, data, preview.Epoch);
                _metrics.RecordMessageReceived(preview.RequestId);
                return Task.CompletedTask;
            }

            // Response arrived before confirmation — buffer it and re-check.
            // This happens when the handler publishes its first response faster than the
            // confirmation round-trip back to the subscriber.
            var buffer = _pendingResponses.GetOrAdd(
                preview.RequestId,
                _ => new ConcurrentQueue<(byte[], long)>());

            buffer.Enqueue((data, preview.Epoch));

            // Evict the oldest entries if buffer exceeds limit (guards against confirmation never arriving)
            var dropped = 0;
            while (buffer.Count > _settings.Subscriber.PendingResponseBufferLimit)
            {
                buffer.TryDequeue(out _);
                dropped++;
            }

            if (dropped > 0)
                _logger.LogWarning(
                    "Dropped {Count} buffered response(s) for requestId '{RequestId}' — " +
                    "confirmation is taking longer than PendingResponseBufferLimit ({Limit})",
                    dropped, preview.RequestId, _settings.Subscriber.PendingResponseBufferLimit);

            // Re-check: confirmation may have arrived between the ContainsKey above and the Enqueue.
            // If so, drain immediately so this message is not stuck until the next publish.
            if (_byRequestId.ContainsKey(preview.RequestId))
                DrainPendingResponses(preview.RequestId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error routing response for '{StreamName}'", streamName);
        }

        return Task.CompletedTask;
    }

    private void DrainPendingResponses(string requestId)
    {
        if (!_pendingResponses.TryRemove(requestId, out var buffer))
            return;

        while (buffer.TryDequeue(out var item))
            _shardDispatcher?.Dispatch(requestId, item.Data, item.Epoch);
    }

    private async Task ProcessResponseAsync(Response response)
    {
        try
        {
            var message    = _serializer.Deserialize<InternalResponseMessage<TResponse>>(response.Data);
            var latencyMs  = message.Timestamp != default
                ? Math.Max(0, (DateTime.UtcNow - message.Timestamp).TotalMilliseconds)
                : 0;

            if (!_byRequestId.TryGetValue(message.RequestId, out var states))
                return;

            _metrics.RecordMessageLatency(message.RequestId, latencyMs);

            List<SubscriptionState<TResponse>> snapshot;
            lock (states) { snapshot = [..states]; }

            foreach (var state in snapshot.Where(state => !state.IsDisposed))
            {
                if (message.Epoch < state.LastKnownEpoch)
                {
                    _logger.LogWarning(
                        "Rejecting stale message for '{RequestId}' (epoch {Msg} < {Known})",
                        message.RequestId, message.Epoch, state.LastKnownEpoch);
                    continue;
                }

                state.LastKnownEpoch = message.Epoch;

                // Time-to-first-response: measured from when the subscriber sent the request
                // to when the first full snapshot arrives (LocalImage still null at that point).
                if (state.LocalImage == null)
                {
                    var firstResponseMs = (DateTime.UtcNow - state.RequestedAt).TotalMilliseconds;
                    _metrics.RecordFirstResponseLatency(message.RequestId, Math.Max(0, firstResponseMs));

                    _logger.LogInformation(
                        "First response on '{StreamName}' [{RequestId}] — {FirstResponseMs:F0}ms to first message",
                        streamName, message.RequestId, firstResponseMs);
                }

                // Merge delta onto local image before delivering to handler.
                // ChangedProperties == null → full snapshot (first message or close): set directly.
                // ChangedProperties != null → delta: merge only changed properties.
                TResponse merged;
                if (message.ChangedProperties == null)
                {
                    state.LocalImage = message.Data;
                    merged = message.Data;
                }
                else
                {
                    state.LocalImage = _diffComputer.Merge(
                        state.LocalImage ?? message.Data,
                        message.Data,
                        message.ChangedProperties);
                    merged = state.LocalImage;
                }

                state.Subject.OnNext(merged);

                _metrics.RecordLatestImage(
                    state.RequestId!,
                    streamName,
                    JsonSerializer.Serialize(merged));
                
                if (message.IsFinal)
                    await HandleFinalMessageAsync(state, message.CloseReason ?? CloseReason.Normal);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error processing response for RequestId '{RequestId}'", response.RequestId);
        }
    }

    private async Task HandleFinalMessageAsync(
        SubscriptionState<TResponse> state,
        CloseReason reason)
    {
        var shouldReconnect = reason is
            CloseReason.Error or
            CloseReason.Timeout or
            CloseReason.Orphaned or
            CloseReason.Shutdown;

        _logger.LogInformation(
            "Stream closed on '{StreamName}' [{RequestId}] — reason: {Reason}{Reconnect}",
            streamName, state.RequestId, reason,
            shouldReconnect ? " (will reconnect)" : "");

        if (shouldReconnect)
        {
            // Signal reconnect needed - StreamingSubscriber handles it via Polly
            state.NotifyStatus(StreamStatus.Reconnecting(
                streamName,
                ++state.ReconnectAttempts,
                _settings.Subscriber.MaxReconnectAttempts,
                new PublisherUnavailableException($"Stream closed: {reason}")));
        }
        else
        {
            // Normal/Unsubscribe - complete observable cleanly
            state.Subject.OnCompleted();
            await UnregisterAsync(state);
        }
    }

    #endregion

    #region Transport Subscription Management

    private async Task EnsureTransportSubscribedAsync(CancellationToken cancellationToken)
    {
        if (_transportSubscribed) return;

        await _subscriptionLock.WaitAsync(cancellationToken);
        try
        {
            if (_transportSubscribed) return;

            // Get subject names from resolver
            var responsesSubject = _subjects.GetResponsesWildcard(streamName);
            var confirmSubject = _subjects.GetConfirmSubject(streamName);
            var keepaliveSubject = _subjects.GetKeepaliveSubject(streamName);

            // Subscribe to transport
            await _transport.SubscribeAsync(confirmSubject, OnConfirmationReceivedAsync, cancellationToken);
            await _transport.SubscribeAsync(responsesSubject, OnResponseReceivedAsync, cancellationToken);
            await _transport.SubscribeAsync(
                keepaliveSubject,
                OnKeepaliveReceivedAsync,
                cancellationToken);

            // Initialize shard dispatcher for parallel response processing
            _shardDispatcher = new ResponseShardDispatcher(
                _settings.Subscriber.DispatchWorkerCount,
                _settings.Subscriber.DispatchChannelCapacity,
                ProcessResponseAsync,
                _logger);

            _transportSubscribed = true;

            _logger.LogInformation(
                "Transport subscriptions active for '{StreamName}' " +
                "(confirm: '{ConfirmSubject}', responses: '{ResponsesSubject}')",
                streamName, confirmSubject, responsesSubject);
        }
        finally
        {
            _subscriptionLock.Release();
        }
    }

    private async Task UnsubscribeFromTransportAsync()
    {
        await _subscriptionLock.WaitAsync();
        try
        {
            if (!_transportSubscribed) return;

            // Get subject names from resolver
            var responsesSubject = _subjects.GetResponsesWildcard(streamName);
            var confirmSubject = _subjects.GetConfirmSubject(streamName);
            var keepaliveSubject = _subjects.GetKeepaliveSubject(streamName);

            // Unsubscribe from transport
            await _transport.UnsubscribeAsync(confirmSubject);
            await _transport.UnsubscribeAsync(responsesSubject);
            await _transport.UnsubscribeAsync(keepaliveSubject);

            // Dispose shard dispatcher
            if (_shardDispatcher != null)
            {
                await _shardDispatcher.DisposeAsync();
                _shardDispatcher = null;
            }

            _transportSubscribed = false;

            _logger.LogInformation(
                "Transport subscriptions removed for '{StreamName}'", streamName);
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
            var unsubscribeSubject = _subjects.GetUnsubscribeSubject(streamName);

            var envelope = new UnsubscribeEnvelope
            {
                RequestId = requestId,
                StreamName = streamName,
                SubscriberId = options.Value.InstanceId,
                UnsubscribedAt = DateTime.UtcNow
            };

            var data = _serializer.Serialize(envelope);
            await _transport.PublishAsync(unsubscribeSubject, data, cancellationToken);

            _logger.LogInformation(
                "Unsubscribed from '{StreamName}' — requestId '{RequestId}'",
                streamName, requestId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to send unsubscribe for RequestId '{RequestId}'", requestId);
        }
    }

    #endregion

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        StopWatchdog();
        StopHeartbeat();
        foreach (var states in _byRequestId.Values)
            lock (states)
                foreach (var s in states)
                    s.Subject.OnCompleted();

        foreach (var state in _byCorrelationId.Values)
            state.Subject.OnCompleted();

        await UnsubscribeFromTransportAsync();
        _subscriptionLock.Dispose();

        _logger.LogInformation("SubscriptionManager disposed for '{StreamName}'", streamName);
    }
}