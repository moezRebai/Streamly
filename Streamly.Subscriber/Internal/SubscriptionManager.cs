using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Models;
using Streamly.Infrastructure.Interfaces;
using Streamly.Subscriber.Configuration;
using Streamly.Subscriber.Models;

namespace Streamly.Subscriber.Internal;

internal class SubscriptionManager<TRequest, TResponse>(
    string streamName,
    IStreamingTransport transport,
    ISubjectResolver subjects,
    IMessageSerializer serializer,
    IOptions<SubscriberOptions> options,
    ILogger<SubscriptionManager<TRequest, TResponse>> logger)
    : IAsyncDisposable
{
    private readonly IStreamingTransport _transport = transport ?? throw new ArgumentNullException(nameof(transport));
    private readonly ISubjectResolver _subjects = subjects ?? throw new ArgumentNullException(nameof(subjects));  
    private readonly IMessageSerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly SubscriberOptions _options = options.Value ?? throw new ArgumentNullException(nameof(options));
    private readonly ILogger<SubscriptionManager<TRequest, TResponse>> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly ConcurrentDictionary<string, List<SubscriptionState<TResponse>>> _byRequestId = new();
    private readonly ConcurrentDictionary<string, SubscriptionState<TResponse>> _byCorrelationId = new();

    private DispatchWorkerPool<TResponse>? _workerPool;
    private int _activeSubscriptionCount;
    private bool _transportSubscribed;
    private readonly SemaphoreSlim _subscriptionLock = new(1, 1);
    private bool _disposed;

    // ── Watchdog ─────────────────────────────────────────────────────────────
    private DateTime _lastResponseTime = DateTime.UtcNow;
    private CancellationTokenSource? _watchdogCts;
    private Task? _watchdogTask;

    public async Task RegisterPendingAsync(
        SubscriptionState<TResponse> state,
        CancellationToken cancellationToken)
    {
        await EnsureTransportSubscribedAsync(cancellationToken);

        _byCorrelationId[state.CorrelationId] = state;
        Interlocked.Increment(ref _activeSubscriptionCount);

        // Start watchdog on first registration
        StartWatchdog();

        _logger.LogDebug(
            "Registered pending subscription '{CorrelationId}' for stream '{StreamName}'",
            state.CorrelationId, streamName);
    }

    public async Task UnregisterAsync(
        SubscriptionState<TResponse> state,
        CancellationToken cancellationToken = default)
    {
        if (state.IsDisposed) return;
        state.IsDisposed = true;

        if (state.WaitingForConfirmation)
            _byCorrelationId.TryRemove(state.CorrelationId, out _);

        if (state.RequestId != null)
        {
            RemoveFromRequestIdDict(state);

            if (!_byRequestId.TryGetValue(state.RequestId, out var value) || value.Count == 0)
            {
                await SendUnsubscribeAsync(state.RequestId, cancellationToken);
            }
        }

        Interlocked.Decrement(ref _activeSubscriptionCount);

        if (_activeSubscriptionCount <= 0)
        {
            StopWatchdog();
            await UnsubscribeFromTransportAsync();
        }
    }

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
            streamName, _options.HeartbeatTimeout.TotalMilliseconds);
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
                if (elapsed <= _options.HeartbeatTimeout) continue;

                _logger.LogWarning(
                    "Heartbeat timeout on '{StreamName}': no response for {Elapsed}ms " +
                    "(threshold: {Threshold}ms). Notifying subscribers.",
                    streamName,
                    (int)elapsed.TotalMilliseconds,
                    (int)_options.HeartbeatTimeout.TotalMilliseconds);

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
    /// Polly in StreamingSubscriber will retry and call OnStreamRestored when back.
    /// </summary>
    private void NotifyReconnecting()
    {
        var activeStates = _byRequestId.Values
            .SelectMany(list => { lock (list) return list.ToList(); })
            .Concat(_byCorrelationId.Values)
            .Distinct()
            .ToList();

        foreach (var state in activeStates)
        {
            if (state.IsDisposed) continue;

            state.ReconnectAttempts++;

            state.NotifyStatus(StreamStatus.Reconnecting(
                streamName,
                state.ReconnectAttempts,
                _options.MaxReconnectAttempts,
                new PublisherUnavailableException(
                    $"No response from '{streamName}' for {(int)_options.HeartbeatTimeout.TotalMilliseconds}ms")));
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
            state.WaitingForConfirmation = false;

            _byRequestId.AddOrUpdate(
                confirmation.RequestId,
                _ => [state],
                (_, existing) =>
                {
                    lock (existing) { existing.Add(state); }
                    return existing;
                });

            _logger.LogInformation(
                "Confirmed: correlationId '{CorrelationId}' → requestId '{RequestId}'",
                confirmation.CorrelationId, confirmation.RequestId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing confirmation for '{StreamName}'", streamName);
        }

        return Task.CompletedTask;
    }

    private Task OnResponseReceivedAsync(byte[] data)
    {
        try
        {
            // ✅ Reset watchdog on every message received
            _lastResponseTime = DateTime.UtcNow;

            var preview = _serializer.Deserialize<ResponsePreview>(data);

            if (!_byRequestId.ContainsKey(preview.RequestId))
                return Task.CompletedTask;

            _workerPool?.Dispatch(preview.RequestId, data, preview.Epoch);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error routing response for '{StreamName}'", streamName);
        }

        return Task.CompletedTask;
    }

    private async Task ProcessDispatchItemAsync(DispatchItem item)
    {
        try
        {
            var message = _serializer.Deserialize<InternalResponseMessage<TResponse>>(item.Data);

            if (!_byRequestId.TryGetValue(message.RequestId, out var states))
                return;

            List<SubscriptionState<TResponse>> snapshot;
            lock (states) { snapshot = [..states]; }

            foreach (var state in snapshot)
            {
                if (state.IsDisposed) continue;

                if (message.Epoch < state.LastKnownEpoch)
                {
                    _logger.LogWarning(
                        "Rejecting stale message for '{RequestId}' (epoch {Msg} < {Known})",
                        message.RequestId, message.Epoch, state.LastKnownEpoch);
                    continue;
                }

                state.LastKnownEpoch = message.Epoch;
                state.Subject.OnNext(message.Data);

                if (message.IsFinal)
                    await HandleFinalMessageAsync(state, message.CloseReason ?? CloseReason.Normal);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error processing dispatch item for RequestId '{RequestId}'", item.RequestId);
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

        if (shouldReconnect)
        {
            // Signal reconnect needed - StreamingSubscriber handles it via Polly
            state.NotifyStatus(StreamStatus.Reconnecting(
                streamName,
                ++state.ReconnectAttempts,
                _options.MaxReconnectAttempts,
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

            // Subscribe to transport
            await _transport.SubscribeAsync(confirmSubject, OnConfirmationReceivedAsync, cancellationToken);
            await _transport.SubscribeAsync(responsesSubject, OnResponseReceivedAsync, cancellationToken);

            // Initialize worker pool for parallel dispatch
            _workerPool = new DispatchWorkerPool<TResponse>(
                _options.DispatchWorkerCount,
                _options.DispatchChannelCapacity,
                ProcessDispatchItemAsync,
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
            
            // Unsubscribe from transport
            await _transport.UnsubscribeAsync(confirmSubject);
            await _transport.UnsubscribeAsync(responsesSubject);

            // Dispose worker pool
            if (_workerPool != null)
            {
                await _workerPool.DisposeAsync();
                _workerPool = null;
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
                UnsubscribedAt = DateTime.UtcNow
            };

            var data = _serializer.Serialize(envelope);
            await _transport.PublishAsync(unsubscribeSubject, data, cancellationToken);

            _logger.LogInformation(
                "Sent unsubscribe for RequestId '{RequestId}' to '{Subject}'",
                requestId, unsubscribeSubject);
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