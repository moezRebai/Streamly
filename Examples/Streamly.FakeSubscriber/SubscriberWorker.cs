// ─────────────────────────────────────────────────────────────────────────────
// FILE: Streamly.Test.Subscriber/SubscriberTestWorker.cs
// Shows how client uses onStatusChanged to react to stream loss/restore
// ─────────────────────────────────────────────────────────────────────────────

using Streamly.Core.Models;
using Streamly.Subscriber.Models;

namespace Streamly.Subscriber;

public class SubscriberWorker(
    IStreamingSubscriber<SpotRequest, SpotPrice> subscriber,
    ILogger<SubscriberWorker> logger)
    : BackgroundService
{
    private IDisposable? _clientA;
    private IDisposable? _clientB;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Starting subscriber test - two clients");

        var index = 0;
        // ── Client A: EUR/USD ────────────────────────────────────────────────
        _clientA = subscriber
            .Subscribe(
                new SpotRequest { CurrencyPair = "EUR/USD" },
                behavior: StreamBehavior.Live,
                onStatusChanged: status =>
                {
                    switch (status.State)
                    {
                        case StreamState.Reconnecting:
                            logger.LogWarning(
                                "[Client A] EUR/USD stream lost - {Message}",
                                status.Message);
                            break;

                        case StreamState.Active when status.RetryAttempt > 0:
                            logger.LogInformation(
                                "[Client A] EUR/USD stream restored after {Attempts} attempt(s)",
                                status.RetryAttempt);
                            break;

                        case StreamState.Failed:
                            logger.LogError(
                                "[Client A] EUR/USD stream permanently lost: {Message}",
                                status.Message);
                            break;
                    }
                })
            .Subscribe(
                onNext: price =>
                {
                    logger.LogInformation(
                        "[Client A] EUR/USD → Bid: {Bid:F5}  Ask: {Ask:F5}  @ {Time:HH:mm:ss.fff}",
                        price.Bid, price.Ask, price.Timestamp);
                    index++;
                },
                    
                onError: ex =>
                    logger.LogError(ex, "[Client A] EUR/USD permanently failed"),
                onCompleted: () =>
                    logger.LogInformation("[Client A] EUR/USD stream completed normally"));

        await Task.Delay(200, stoppingToken);

        // ── Client B: EUR/GBP ────────────────────────────────────────────────
        /*_clientB = subscriber
            .Subscribe(
                new SpotRequest { CurrencyPair = "EUR/GBP" },
                behavior: StreamBehavior.Live,
                onStatusChanged: status =>
                {
                    switch (status.State)
                    {
                        case StreamState.Reconnecting:
                            logger.LogWarning(
                                "[Client B] EUR/GBP stream lost - {Message}",
                                status.Message);
                            break;

                        case StreamState.Active when status.RetryAttempt > 0:
                            logger.LogInformation(
                                "[Client B] EUR/GBP stream restored after {Attempts} attempt(s)",
                                status.RetryAttempt);
                            break;

                        case StreamState.Failed:
                            logger.LogError(
                                "[Client B] EUR/GBP stream permanently lost: {Message}",
                                status.Message);
                            break;
                    }
                })
            .Subscribe(
                onNext: price =>
                    logger.LogInformation(
                        "[Client B] EUR/GBP → Bid: {Bid:F5}  Ask: {Ask:F5}  @ {Time:HH:mm:ss.fff}",
                        price.Bid, price.Ask, price.Timestamp),
                onError: ex =>
                    logger.LogError(ex, "[Client B] EUR/GBP permanently failed"),
                onCompleted: () =>
                    logger.LogInformation("[Client B] EUR/GBP stream completed normally"));

        logger.LogInformation("Both clients active. Waiting for prices...");*/

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stopping - disposing clients");
        _clientA?.Dispose();
        _clientB?.Dispose();
        await base.StopAsync(cancellationToken);
    }
}