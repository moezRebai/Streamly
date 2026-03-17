using System.Reactive.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;
using Streamly.Core.Models;
using Streamly.Infrastructure.Interfaces;
using Streamly.Subscriber.Configuration;
using Streamly.Subscriber.Internal;
using Streamly.Subscriber.Models;

namespace Streamly.Subscriber;

// ─────────────────────────────────────────────────────────────────────────────
// FILE: Streamly.Subscriber/StreamingSubscriber.cs
// CHANGES:
//   1. Stream() accepts onStatusChanged callback
//   2. State gets StatusCallback wired before registration
//   3. NotifyRestored() called after successful reconnect
//   4. Final failure calls OnError on observer (all retries exhausted)
// ─────────────────────────────────────────────────────────────────────────────

internal class StreamingSubscriber<TRequest, TResponse>
    : IStreamingSubscriber<TRequest, TResponse>, IAsyncDisposable
{
    private readonly SubscriptionManager<TRequest, TResponse> _subscriptionManager;
    private readonly IMessageSerializer _serializer;
    private readonly SubscriberOptions _options;
    private readonly ILogger<StreamingSubscriber<TRequest, TResponse>> _logger;
    private readonly IStreamingTransport _transport;
    private readonly ISubjectResolver _subjects;
    private readonly string _streamName;
    private bool _disposed;
    private readonly StreamlyRuntimeOptions _runtimeOptions;

    public StreamingSubscriber(
        string streamName,
        SubscriptionManager<TRequest, TResponse> subscriptionManager,
        IMessageSerializer serializer,
        IStreamingTransport transport,
        ISubjectResolver subjects,
        IOptions<SubscriberOptions> options,
        IOptions<StreamlyRuntimeOptions> runtimeOptions,
        ILogger<StreamingSubscriber<TRequest, TResponse>> logger)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));

        _subscriptionManager = subscriptionManager ?? throw new ArgumentNullException(nameof(subscriptionManager));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _options = options.Value ?? throw new ArgumentNullException(nameof(options));
        _runtimeOptions = runtimeOptions.Value ?? throw new ArgumentNullException(nameof(runtimeOptions));
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
            var cycleAttempt = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                cycleAttempt++;

                // Log reconnect attempts (not the first connection)
                if (cycleAttempt > 1)
                {
                    _logger.LogInformation(
                        "Reconnect attempt {Attempt}/{Max} for '{StreamName}'",
                        cycleAttempt, _options.MaxReconnectAttempts, _streamName);
                }

                // All retries exhausted
                if (cycleAttempt > _options.MaxReconnectAttempts)
                {
                    var ex = new PublisherUnavailableException(
                        $"All {_options.MaxReconnectAttempts} reconnect attempts exhausted for '{_streamName}'");

                    _logger.LogError(ex,
                        "Stream permanently lost for '{StreamName}' after {Max} attempts",
                        _streamName, _options.MaxReconnectAttempts);

                    onStatusChanged?.Invoke(
                        StreamStatus.Failed(_streamName, _options.MaxReconnectAttempts, ex));

                    observer.OnError(ex);
                    break;
                }

                try
                {
                    await DoSubscribeAsync(
                        request, behavior, observer, onStatusChanged,
                        isReconnect: cycleAttempt > 1,
                        cancellationToken);

                    // DoSubscribeAsync returned cleanly = stream ended normally
                    // (publisher sent OnCompleted / unsubscribe)
                    _logger.LogInformation("Stream ended normally for '{StreamName}'", _streamName);
                    break;
                }
                catch (OperationCanceledException)
                {
                    // User disposed the subscription — stop everything
                    _logger.LogInformation("Subscription cancelled for '{StreamName}'", _streamName);
                    break;
                }
                catch (PublisherUnavailableException ex)
                {
                    // Was confirmed and connected, then publisher died
                    // → reset counter because we proved the system works
                    _logger.LogWarning("Publisher lost on '{StreamName}' after successful connection, " +
                        "resetting retry counter",
                        _streamName);

                    cycleAttempt = 0; // next iteration = attempt 1
                    
                    var delay = CalculateDelay();
                    await SafeDelay(delay, cancellationToken);
                }
                catch (TimeoutException ex)
                {
                    // Never got confirmation — publisher not ready yet
                    // → keep incrementing, apply backoff
                    _logger.LogWarning("No confirmation for '{StreamName}' (attempt {Attempt}/{Max}), " +
                        "retrying in {Delay:F1}s",
                        _streamName, cycleAttempt, _options.MaxReconnectAttempts, CalculateDelay().TotalSeconds);

                    await SafeDelay(CalculateDelay(), cancellationToken);
                }
                catch (Exception ex)
                {
                    // Unexpected error — treat like timeout, keep counting
                    _logger.LogError(ex,
                        "Unexpected error on '{StreamName}' (attempt {Attempt}/{Max})",
                        _streamName, cycleAttempt, _options.MaxReconnectAttempts);

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
        CancellationToken cancellationToken)
    {
        var correlationId = Guid.NewGuid().ToString("N");

        // Per-attempt CTS — watchdog cancels this to wake up tcs.Task
        // Linked to cancellationToken so user dispose also cancels it
        using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var state = new SubscriptionState<TResponse>
        {
            CorrelationId = correlationId,
            Behavior = behavior,
            StatusCallback = onStatusChanged
        };

        using var subscription = state.Subject.Subscribe(
            onNext: observer.OnNext,
            onError: observer.OnError,
            onCompleted: observer.OnCompleted);

        // Register with manager — sets up confirm/response transport subscriptions
        // and gives watchdog the attemptCts to cancel when heartbeat times out
        await _subscriptionManager.RegisterPendingAsync(state, attemptCts, cancellationToken);

        try
        {
            // Step 1 — publish request to publisher
            var envelope = new RequestEnvelope<TRequest>
            {
                Request = request,
                Behavior = behavior,
                SubscriberId = _runtimeOptions.InstanceId,
                CorrelationId = correlationId,
                SubscribedAt = DateTime.UtcNow
            };

            var data = _serializer.Serialize(envelope);
            var requestsSubject = _subjects.GetRequestsSubject(_streamName);
            await _transport.PublishAsync(requestsSubject, data, cancellationToken);

            _logger.LogDebug(
                "Published {Behavior} request to '{Subject}' (correlationId: {CorrelationId})",
                behavior, requestsSubject, correlationId);

            // Step 2 — wait for publisher confirmation
            // TimeoutException thrown here if no confirmation within ConfirmationTimeout
            var confirmed = await WaitForConfirmationAsync(state, cancellationToken);

            if (!confirmed)
                throw new TimeoutException(
                    $"No confirmation within {_options.ConfirmationTimeout.TotalSeconds}s " +
                    $"for '{_streamName}'");

            // Step 3 — confirmed, stream is now active
            // Notify restored only if this was a reconnect attempt
            if (isReconnect)
            {
                _subscriptionManager.NotifyRestored();
                onStatusChanged?.Invoke(StreamStatus.Active(_streamName));
            }

            _logger.LogInformation(
                "Stream active: RequestId '{RequestId}' on '{StreamName}'",
                state.RequestId, _streamName);

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
            await _subscriptionManager.UnregisterAsync(state, CancellationToken.None);
        }
    }

    private TimeSpan CalculateDelay()
    {
        // Constant delay with small jitter to avoid thundering herd
        var jitter = Random.Shared.NextDouble() * 0.4 - 0.2; // ±20%
        return TimeSpan.FromMilliseconds(
            _options.ReconnectInitialDelay.TotalMilliseconds * (1 + jitter));
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
        timeoutCts.CancelAfter(_options.ConfirmationTimeout);

        try
        {
            while (state.WaitingForConfirmation && !timeoutCts.Token.IsCancellationRequested)
                await Task.Delay(5, timeoutCts.Token);

            return !state.WaitingForConfirmation;
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