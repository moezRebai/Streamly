using System.Reactive.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using Polly;
using Polly.Retry;
using Streamly.Core.Abstractions;
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
    private readonly ResiliencePipeline _reconnectPolicy;
    private bool _disposed;

    public StreamingSubscriber(
        string streamName,
        SubscriptionManager<TRequest, TResponse> subscriptionManager,
        IMessageSerializer serializer,
        IStreamingTransport transport,
        ISubjectResolver subjects,
        IOptions<SubscriberOptions> options,
        ILogger<StreamingSubscriber<TRequest, TResponse>> logger)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));

        _subscriptionManager = subscriptionManager ?? throw new ArgumentNullException(nameof(subscriptionManager));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _options = options.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _subjects = subjects ?? throw new ArgumentNullException(nameof(subjects));
        _streamName = streamName;
        _reconnectPolicy = BuildReconnectPolicy();
    }

    public IObservable<TResponse> Subscribe(
        TRequest request,
        StreamBehavior behavior = StreamBehavior.Live,
        Action<StreamStatus>? onStatusChanged = null)
    {
        if (request == null) throw new ArgumentNullException(nameof(request));

        return Observable.Create<TResponse>(async (observer, cancellationToken) =>
        {
            var attempt = 0;

            await _reconnectPolicy.ExecuteAsync(async ct =>
            {
                attempt++;

                // Notify reconnecting (skip first attempt - that's not a reconnect)
                if (attempt > 1)
                {
                    _logger.LogInformation(
                        "Reconnect attempt {Attempt}/{Max} for '{StreamName}'",
                        attempt, _options.MaxReconnectAttempts, _streamName);
                }

                await DoSubscribeAsync(request, behavior, observer, onStatusChanged, ct);

                // If we get here after a retry, notify restored
                if (attempt > 1)
                {
                    _subscriptionManager.NotifyRestored();
                    onStatusChanged?.Invoke(StreamStatus.Active(_streamName));
                }

            }, cancellationToken);
        });
    }

    private async Task DoSubscribeAsync(
        TRequest request,
        StreamBehavior behavior,
        IObserver<TResponse> observer,
        Action<StreamStatus>? onStatusChanged,
        CancellationToken cancellationToken)
    {
        var correlationId = Guid.NewGuid().ToString("N");

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

        await _subscriptionManager.RegisterPendingAsync(state, cancellationToken);

        var envelope = new RequestEnvelope<TRequest>
        {
            Request = request,
            Behavior = behavior,
            CorrelationId = correlationId,
            SubscribedAt = DateTime.UtcNow
        };

        var data = _serializer.Serialize(envelope);
        var requestsSubject = _subjects.GetRequestsSubject(_streamName);
        await _transport.PublishAsync(requestsSubject, data, cancellationToken);

        _logger.LogDebug(
            "Published {Behavior} request to '{Channel}' (correlationId: {CorrelationId})",
            behavior, requestsSubject, correlationId);

        var confirmed = await WaitForConfirmationAsync(state, cancellationToken);

        if (!confirmed)
        {
            await _subscriptionManager.UnregisterAsync(state, cancellationToken);
            throw new TimeoutException(
                $"No confirmation within {_options.ConfirmationTimeout.TotalSeconds}s for '{_streamName}'");
        }

        _logger.LogInformation(
            "Stream active: RequestId '{RequestId}' on '{StreamName}'",
            state.RequestId, _streamName);

        // Keep alive until cancelled or stream ends
        var tcs = new TaskCompletionSource();

        using var completionSub = state.Subject.Subscribe(
            onNext: _ => { },
            onError: _ => tcs.TrySetResult(),
            onCompleted: () => tcs.TrySetResult());

        await using var cancelReg = cancellationToken.Register(() => tcs.TrySetResult());

        await tcs.Task;

        if (!state.IsDisposed)
            await _subscriptionManager.UnregisterAsync(state, cancellationToken);
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

    private ResiliencePipeline BuildReconnectPolicy()
    {
        return new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder()
                    .Handle<TimeoutException>()
                    .Handle<PublisherUnavailableException>()
                    .Handle<NatsServerException>() 
                    .Handle<Exception>(ex => ex is not OperationCanceledException),

                MaxRetryAttempts = _options.MaxReconnectAttempts,
                BackoffType = DelayBackoffType.Exponential,
                Delay = _options.ReconnectInitialDelay,
                MaxDelay = _options.ReconnectMaxDelay,
                UseJitter = true,

                OnRetry = args =>
                {
                    _logger.LogWarning(
                        "Reconnect attempt {Attempt}/{Max} for '{StreamName}' in {Delay:F1}s - {Reason}",
                        args.AttemptNumber + 1,
                        _options.MaxReconnectAttempts,
                        _streamName,
                        args.RetryDelay.TotalSeconds,
                        args.Outcome.Exception?.Message);

                    return ValueTask.CompletedTask;
                }
            })
            .Build();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _subscriptionManager.DisposeAsync();
    }
}