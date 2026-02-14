using System.Reactive.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Streamly.Core.Models;
using Streamly.Core.Runtime.Channel;
using Streamly.Core.Runtime.Registration;
using Streamly.Infrastructure.Interfaces;
using Streamly.Subscriber.Configuration;
using Streamly.Subscriber.Internal;

namespace Streamly.Subscriber;

/// <summary>
/// Client-side implementation of streaming subscriptions
///
/// What the client sees:
///   var stream = subscriber.Stream(request, StreamBehavior.Live);
///   stream.Subscribe(onNext: price => ..., onError: ex => ..., onCompleted: () => ...);
///
/// What happens internally:
///   1. Library generates CorrelationId (hidden from client)
///   2. Publishes RequestEnvelope to Redis
///   3. Waits for leader confirmation (gets real RequestId)
///   4. Routes responses from Redis to observer via SubscriptionManager
///   5. Handles reconnection transparently via Polly
///
/// ONE instance per TRequest/TResponse pair
/// Registered as singleton in DI
/// </summary>
internal class StreamingSubscriber<TRequest, TResponse>
    : IStreamingSubscriber<TRequest, TResponse>, IAsyncDisposable
{
    private readonly SubscriptionManager<TRequest, TResponse> _subscriptionManager;
    private readonly IRedisConnectionManager _redis;
    private readonly IMessageSerializer _serializer;
    private readonly IChannelNameResolver _channelResolver;
    private readonly SubscriberOptions _options;
    private readonly ILogger<StreamingSubscriber<TRequest, TResponse>> _logger;

    private readonly string _streamName;
    private readonly string _requestsChannel;
    private ResiliencePipeline _reconnectPolicy;
    private bool _disposed;

    public StreamingSubscriber(
        SubscriptionManager<TRequest, TResponse> subscriptionManager,
        IStreamRegistry streamRegistry,
        IRedisConnectionManager redis,
        IMessageSerializer serializer,
        IChannelNameResolver channelResolver,
        IOptions<SubscriberOptions> options,
        ILogger<StreamingSubscriber<TRequest, TResponse>> logger)
    {
        _subscriptionManager = subscriptionManager
            ?? throw new ArgumentNullException(nameof(subscriptionManager));
        _redis = redis ?? throw new ArgumentNullException(nameof(redis));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _channelResolver = channelResolver ?? throw new ArgumentNullException(nameof(channelResolver));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _streamName = streamRegistry.GetStreamName<TRequest>();
        _requestsChannel = _channelResolver.GetRequestsChannel(_streamName);

        // Build Polly reconnect policy
        // Triggered on: Error, Timeout, Orphaned, Shutdown
        // NOT triggered on: Normal, Unsubscribe
        _reconnectPolicy = BuildReconnectPolicy();
    }

    /// <summary>
    /// Subscribe to a streaming request
    /// Returns IObservable - client applies Rx operators and subscribes
    /// </summary>
    public IObservable<TResponse> Stream(
        TRequest request,
        StreamBehavior behavior = StreamBehavior.Live)
    {
        if (request == null)
            throw new ArgumentNullException(nameof(request));

        _logger.LogInformation(
            "Creating {Behavior} stream for stream '{StreamName}'",
            behavior,
            _streamName);

        // Return cold observable - subscription starts when client calls .Subscribe()
        return Observable.Create<TResponse>(async (observer, cancellationToken) =>
        {
            await SubscribeInternalAsync(request, behavior, observer, cancellationToken);
        });
    }

    private async Task SubscribeInternalAsync(
        TRequest request,
        StreamBehavior behavior,
        IObserver<TResponse> observer,
        CancellationToken cancellationToken)
    {
        // Use Polly for reconnection
        await _reconnectPolicy.ExecuteAsync(async (ctx, ct) =>
        {
            await DoSubscribeAsync(request, behavior, observer, ct);
        },
        new Context($"stream-{_streamName}"),
        cancellationToken);
    }

    private async Task DoSubscribeAsync(
        TRequest request,
        StreamBehavior behavior,
        IObserver<TResponse> observer,
        CancellationToken cancellationToken)
    {
        // Step 1: Create internal subscription state
        var correlationId = Guid.NewGuid().ToString("N"); // Hidden from client

        var state = new SubscriptionState<TResponse>
        {
            CorrelationId = correlationId,
            Behavior = behavior
        };

        // Step 2: Bridge Subject to observer
        using var subscription = state.Subject.Subscribe(
            onNext: observer.OnNext,
            onError: observer.OnError,
            onCompleted: observer.OnCompleted);

        // Step 3: Register with SubscriptionManager (waits for confirmation)
        await _subscriptionManager.RegisterPendingAsync(state, cancellationToken);

        // Step 4: Build and publish RequestEnvelope to Redis
        var envelope = new RequestEnvelope<TRequest>
        {
            Request = request,
            Behavior = behavior,
            CorrelationId = correlationId, // Internal - leader uses this to confirm back
            SubscribedAt = DateTime.UtcNow
        };

        var data = _serializer.Serialize(envelope);
        await _redis.PublishAsync(_requestsChannel, data, cancellationToken);

        _logger.LogDebug(
            "Published {Behavior} request to '{Channel}' (correlationId: {CorrelationId})",
            behavior,
            _requestsChannel,
            correlationId);

        // Step 5: Wait for confirmation (leader assigns real RequestId)
        var confirmed = await WaitForConfirmationAsync(state, cancellationToken);

        if (!confirmed)
        {
            // Confirmation timeout - cleanup and let Polly retry
            await _subscriptionManager.UnregisterAsync(state, cancellationToken);

            _logger.LogWarning(
                "Confirmation timeout for correlationId '{CorrelationId}', will retry",
                correlationId);

            throw new TimeoutException(
                $"No confirmation received within {_options.ConfirmationTimeout.TotalSeconds}s");
        }

        _logger.LogInformation(
            "Stream active for RequestId '{RequestId}' (stream '{StreamName}')",
            state.RequestId,
            _streamName);

        // Step 6: Keep alive until cancelled or subject completes
        var tcs = new TaskCompletionSource();
        using var completionSubscription = state.Subject.Subscribe(
            onNext: _ => { },
            onError: _ => tcs.TrySetResult(),
            onCompleted: () => tcs.TrySetResult());

        using var cancelReg = cancellationToken.Register(() => tcs.TrySetResult());

        await tcs.Task;

        // Cleanup on dispose/cancel
        if (state.IsDisposed) return;
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
            // Poll until WaitingForConfirmation = false or timeout
            while (state.WaitingForConfirmation && !timeoutCts.Token.IsCancellationRequested)
            {
                await Task.Delay(10, timeoutCts.Token); // Check every 10ms
            }

            return !state.WaitingForConfirmation; // True = confirmed, False = timeout
        }
        catch (OperationCanceledException)
        {
            return false; // Timeout
        }
    }

    private ResiliencePipeline BuildReconnectPolicy()
    {
        return new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                // Trigger reconnect on these reasons
                ShouldHandle = new PredicateBuilder()
                    .Handle<TimeoutException>()          // Confirmation timeout
                    .Handle<Exception>(ex =>
                        ex is not OperationCanceledException),

                MaxRetryAttempts = _options.MaxReconnectAttempts,

                // Exponential backoff: 1s → 2s → 4s → ... → 30s
                BackoffType = DelayBackoffType.Exponential,
                Delay = _options.ReconnectInitialDelay,
                MaxDelay = _options.ReconnectMaxDelay,
                UseJitter = true, // Avoid thundering herd

                OnRetry = args =>
                {
                    _logger.LogInformation(
                        "Reconnect attempt {Attempt}/{Max} for stream '{StreamName}' " +
                        "after {Delay}s (reason: {Exception})",
                        args.AttemptNumber + 1,
                        _options.MaxReconnectAttempts,
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

        _logger.LogInformation(
            "StreamingSubscriber disposed for stream '{StreamName}'",
            _streamName);
    }
}
