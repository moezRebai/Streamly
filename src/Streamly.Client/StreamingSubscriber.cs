using System.Reactive.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;
using Streamly.Core.Models;
using Streamly.Infrastructure.Interfaces;
using Streamly.Client.Configuration;
using Streamly.Client.Internal;
using Streamly.Client.Models;

namespace Streamly.Client;

internal class StreamingSubscriber<TRequest, TResponse>
    : IStreamingSubscriber<TRequest, TResponse>, IAsyncDisposable
    where TResponse : class
{
    private readonly SubscriptionManager<TRequest, TResponse> _subscriptionManager;
    private readonly IMessageSerializer _serializer;
    private readonly IStreamlyMetricsCollector _metrics;
    private readonly StreamlySettings _settings;
    private readonly ILogger<StreamingSubscriber<TRequest, TResponse>> _logger;
    private readonly IStreamingTransport _transport;
    private readonly ISubjectResolver _subjects;
    private readonly string _streamName;
    private bool _disposed;

    public StreamingSubscriber(
        string streamName,
        SubscriptionManager<TRequest, TResponse> subscriptionManager,
        IMessageSerializer serializer,
        IStreamlyMetricsCollector metrics,
        IStreamingTransport transport,
        ISubjectResolver subjects,
        IOptions<StreamlySettings> options,
        ILogger<StreamingSubscriber<TRequest, TResponse>> logger)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));

        _subscriptionManager = subscriptionManager ?? throw new ArgumentNullException(nameof(subscriptionManager));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _settings = options.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _subjects = subjects ?? throw new ArgumentNullException(nameof(subjects));
        _streamName = streamName;
    }

    public IObservable<TResponse> Subscribe(
        TRequest request,
        StreamBehavior behavior = StreamBehavior.Live,
        Action<StreamStatus>? onStatusChanged = null)
    {
        if (request == null) throw new ArgumentNullException(nameof(request));

        return Observable.Create<TResponse>(async (observer, cancellationToken) =>
        {
            // Assign a GUID tracking key before any confirmation so the subscription
            // is visible in monitoring immediately. Replaced by the real requestId
            // on confirmation via RecordSubscriptionConfirmed.
            var trackingId  = Guid.NewGuid().ToString("N");
            var requestJson = _serializer.SerializeToJson(request);
            _metrics.RecordSubscriptionAttempted(trackingId, _streamName, requestJson);

            var cycleAttempt  = 0;
            var everConfirmed = false;

            while (!cancellationToken.IsCancellationRequested)
            {
                cycleAttempt++;

                // Log reconnect attempts (not the first connection)
                if (cycleAttempt > 1)
                {
                    _logger.LogInformation(
                        "Reconnect attempt {Attempt}/{Max} for '{StreamName}'",
                        cycleAttempt, _settings.Subscriber.MaxReconnectAttempts, _streamName);
                }

                // All retries exhausted
                if (cycleAttempt > _settings.Subscriber.MaxReconnectAttempts)
                {
                    var ex = new PublisherUnavailableException(
                        $"All {_settings.Subscriber.MaxReconnectAttempts} reconnect attempts exhausted for '{_streamName}'");

                    _logger.LogError(ex,
                        "Stream permanently lost for '{StreamName}' after {Max} attempts",
                        _streamName, _settings.Subscriber.MaxReconnectAttempts);

                    // Mark NoProvider only if the stream was never confirmed — meaning no cluster
                    // handled this request. If it was once active, Failed is the right status.
                    if (!everConfirmed)
                        _metrics.RecordSubscriptionClosed(trackingId, CloseReason.NoProvider);

                    onStatusChanged?.Invoke(everConfirmed
                        ? StreamStatus.Failed(_streamName, _settings.Subscriber.MaxReconnectAttempts, ex)
                        : StreamStatus.NoProvider(_streamName));

                    observer.OnError(ex);
                    break;
                }

                try
                {
                    await DoSubscribeAsync(
                        request, behavior, observer, onStatusChanged,
                        isReconnect: cycleAttempt > 1,
                        trackingId,
                        cancellationToken);

                    // DoSubscribeAsync returned cleanly = stream ended normally
                    // (publisher sent OnCompleted / unsubscribe)
                    everConfirmed = true;
                    _logger.LogInformation("Stream ended normally for '{StreamName}'", _streamName);
                    break;
                }
                catch (OperationCanceledException)
                {
                    // User disposed the subscription — stop everything
                    _logger.LogInformation("Subscription cancelled for '{StreamName}'", _streamName);
                    break;
                }
                catch (PublisherUnavailableException)
                {
                    // Was confirmed and connected, then publisher died
                    // → reset counter because we proved the system works
                    everConfirmed = true;
                    _logger.LogWarning("Publisher lost on '{StreamName}' after successful connection, " +
                        "resetting retry counter",
                        _streamName);

                    cycleAttempt = 0; // next iteration = attempt 1

                    var delay = CalculateDelay();
                    await SafeDelay(delay, cancellationToken);
                }
                catch (TimeoutException)
                {
                    // Never got confirmation — publisher not ready yet
                    // → keep incrementing, apply backoff
                    _logger.LogWarning("No confirmation for '{StreamName}' (attempt {Attempt}/{Max}), " +
                        "retrying in {Delay:F1}s",
                        _streamName, cycleAttempt, _settings.Subscriber.MaxReconnectAttempts, CalculateDelay().TotalSeconds);

                    await SafeDelay(CalculateDelay(), cancellationToken);
                }
                catch (Exception ex)
                {
                    // Unexpected error — treat like timeout, keep counting
                    _logger.LogError(ex,
                        "Unexpected error on '{StreamName}' (attempt {Attempt}/{Max})",
                        _streamName, cycleAttempt, _settings.Subscriber.MaxReconnectAttempts);

                    await SafeDelay(CalculateDelay(), cancellationToken);
                }
            }
        });
    }

    private async Task DoSubscribeAsync(
        TRequest request,
        StreamBehavior behavior,
        IObserver<TResponse> observer,
        Action<StreamStatus>? onStatusChanged,
        bool isReconnect,
        string trackingId,
        CancellationToken cancellationToken)
    {
        var correlationId = Guid.NewGuid().ToString("N");

        // Per-attempt CTS — watchdog cancels this to wake up tcs.Task
        // Linked to cancellationToken so user dispose also cancels it
        using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var state = new SubscriptionState<TResponse>
        {
            CorrelationId  = correlationId,
            Behavior       = behavior,
            StatusCallback = onStatusChanged,
            Request        = request,
            TrackingId     = trackingId
        };

        using var subscription = state.Subject.Subscribe(
            onNext: observer.OnNext,
            onError: observer.OnError,
            onCompleted: observer.OnCompleted);

        // Register with manager — sets up confirm/response transport subscriptions
        // and gives watchdog the attemptCts to cancel when heartbeat times out
        await _subscriptionManager.RegisterPendingAsync(state, attemptCts, cancellationToken);

        // Tracks whether the exit is a publisher-death reconnect.
        // Set to true before throwing PublisherUnavailableException so the
        // finally block knows not to send an unsubscribe — the subscriber is
        // reconnecting, not intentionally leaving. The heartbeat mechanism
        // handles genuine disconnects.
        var isPublisherDead = false;

        try
        {
            // Step 1 — publish request to publisher
            var envelope = new RequestEnvelope<TRequest>
            {
                Request = request,
                Behavior = behavior,
                SubscriberId = _settings.InstanceId,
                CorrelationId = correlationId,
                SubscribedAt = DateTime.UtcNow
            };

            var data = _serializer.Serialize(envelope);
            var requestsSubject = _subjects.GetRequestsSubject(_streamName);
            await _transport.PublishAsync(requestsSubject, data, cancellationToken);

            _logger.LogInformation(
                "Subscribing to '{StreamName}' — {@Request}",
                _streamName, request);

            // Step 2 — wait for publisher confirmation
            // TimeoutException thrown here if no confirmation within ConfirmationTimeout
            var confirmed = await WaitForConfirmationAsync(state, cancellationToken);

            if (!confirmed)
                throw new TimeoutException(
                    $"No confirmation within {_settings.Subscriber.ConfirmationTimeoutMs}ms " +
                    $"for '{_streamName}'");

            // Step 3 — confirmed, stream is now active
            // Notify restored only if this was a reconnect attempt
            if (isReconnect)
            {
                _subscriptionManager.NotifyRestored();
                onStatusChanged?.Invoke(StreamStatus.Active(_streamName));
            }

            _logger.LogDebug(
                "Stream active on '{StreamName}' — requestId '{RequestId}'",
                _streamName, state.RequestId);

            // Step 4 — wait here for the duration of the connection lifetime
            // Exits when: stream ends normally, stream errors, or watchdog fires
            var tcs = new TaskCompletionSource();

            using var completionSub = state.Subject.Subscribe(
                onNext: _ => { },
                onError: _ => tcs.TrySetResult(),
                onCompleted: () => tcs.TrySetResult());

            // attemptCts cancelled by watchdog → wakes up tcs.Task
            await using var cancelReg = attemptCts.Token.Register(() => tcs.TrySetResult());

            await tcs.Task;

            // Step 5 — tcs.Task completed, determine why
            if (attemptCts.Token.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                // Watchdog fired — publisher died after successful connection
                // → throw PublisherUnavailableException so Subscribe() resets counter
                isPublisherDead = true;
                throw new PublisherUnavailableException(
                    $"Publisher heartbeat lost on '{_streamName}'");
            }

            // cancellationToken cancelled (user disposed) or stream ended normally
            // → return cleanly, Subscribe() will break the while loop
        }
        finally
        {
            // Always CancellationToken.None — both tokens may be cancelled at this point
            // Must complete cleanup regardless to avoid corrupt SubscriptionManager state
            await _subscriptionManager.UnregisterAsync(
                state,
                sendUnsubscribeMessage: !isPublisherDead,
                CancellationToken.None);
        }
    }

    private TimeSpan CalculateDelay()
    {
        // Constant delay with small jitter to avoid thundering herd
        var jitter = Random.Shared.NextDouble() * 0.4 - 0.2; // ±20%
        return TimeSpan.FromMilliseconds(
            _settings.Subscriber.ReconnectInitialDelayMs * (1 + jitter));
    }

    private static async Task SafeDelay(TimeSpan delay, CancellationToken cancellationToken)
    {
        // Swallows OperationCanceledException so the while loop
        // can check cancellationToken.IsCancellationRequested cleanly
        // instead of throwing out of the catch block
        try
        {
            await Task.Delay(delay, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Let the while condition handle it on next iteration
        }
    }

    private async Task<bool> WaitForConfirmationAsync(
        SubscriptionState<TResponse> state,
        CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_settings.Subscriber.ConfirmationTimeoutMs);

        try
        {
            await state.ConfirmationTcs.Task.WaitAsync(timeoutCts.Token);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _subscriptionManager.DisposeAsync();
    }
}