using Streamly.Client;
using Streamly.Client.Models;
using Streamly.Core.Models;

namespace Streamly.FakeSubscriber;

public class IrsSubscriberWorker(
    IStreamingSubscriber<IrsRequest, IrsResponse> subscriber,
    ILogger<IrsSubscriberWorker> logger)
    : BackgroundService
{
    private IDisposable? _client5Y;
    private IDisposable? _client10Y;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Starting IRS subscriber test - two clients");

        var today = DateOnly.FromDateTime(DateTime.UtcNow);

        // // ── Client A: 5Y USD IRS ─────────────────────────────────────────────
        // _client5Y = subscriber
        //     .Subscribe(
        //         new IrsRequest
        //         {
        //             Tenor         = "5Y",
        //             Notional      = 10_000_000m,
        //             EffectiveDate = today,
        //             MaturityDate  = today.AddYears(5),
        //         },
        //         behavior: StreamBehavior.Live,
        //         onStatusChanged: status =>
        //         {
        //             switch (status.State)
        //             {
        //                 case StreamState.Reconnecting:
        //                     logger.LogWarning(
        //                         "[IRS 5Y] Stream lost - {Message}",
        //                         status.Message);
        //                     break;
        //
        //                 case StreamState.Active when status.RetryAttempt > 0:
        //                     logger.LogInformation(
        //                         "[IRS 5Y] Stream restored after {Attempts} attempt(s)",
        //                         status.RetryAttempt);
        //                     break;
        //
        //                 case StreamState.Failed:
        //                     logger.LogError(
        //                         "[IRS 5Y] Stream permanently lost: {Message}",
        //                         status.Message);
        //                     break;
        //             }
        //         })
        //     .Subscribe(
        //         onNext: price =>
        //         {
        //             logger.LogInformation(
        //                 "[IRS 5Y]  ParRate: {ParRate:P4}  NPV: {Npv:N2}  " +
        //                 "DV01: {Dv01:F4}  Duration: {Duration:F4}  " +
        //                 "FixedPmts: {Fixed}  FloatPmts: {Float}  " +
        //                 "NextFix: {NextFix:dd-MMM-yy}  @ {Time:HH:mm:ss.fff}",
        //                 price.ParRate,
        //                 price.Npv,
        //                 price.Dv01,
        //                 price.Duration,
        //                 price.RemainingFixedPayments,
        //                 price.RemainingFloatPayments,
        //                 price.NextFloatFixingDate,
        //                 price.PricedAtUtc);
        //         },
        //         onError: ex =>
        //             logger.LogError(ex, "[IRS 5Y] Permanently failed"),
        //         onCompleted: () =>
        //             logger.LogInformation("[IRS 5Y] Stream completed normally"));
        //
        // await Task.Delay(200, stoppingToken);

        // ── Client B: 10Y USD IRS ────────────────────────────────────────────
        _client10Y = subscriber
            .Subscribe(
                new IrsRequest
                {
                    Tenor         = "10Y",
                    Notional      = 25_000_000m,
                    EffectiveDate = today,
                    MaturityDate  = today.AddYears(10),
                },
                behavior: StreamBehavior.Live,
                onStatusChanged: status =>
                {
                    switch (status.State)
                    {
                        case StreamState.Reconnecting:
                            logger.LogWarning(
                                "[IRS 10Y] Stream lost - {Message}",
                                status.Message);
                            break;

                        case StreamState.Active when status.RetryAttempt > 0:
                            logger.LogInformation(
                                "[IRS 10Y] Stream restored after {Attempts} attempt(s)",
                                status.RetryAttempt);
                            break;

                        case StreamState.Failed:
                            logger.LogError(
                                "[IRS 10Y] Stream permanently lost: {Message}",
                                status.Message);
                            break;
                    }
                })
            .Subscribe(
                onNext: price =>
                {
                    logger.LogInformation(
                        "[IRS 10Y] ParRate: {ParRate:P4}  NPV: {Npv:N2}  " +
                        "DV01: {Dv01:F4}  Duration: {Duration:F4}  " +
                        "FixedPmts: {Fixed}  FloatPmts: {Float}  " +
                        "NextFix: {NextFix:dd-MMM-yy}  @ {Time:HH:mm:ss.fff}",
                        price.ParRate,
                        price.Npv,
                        price.Dv01,
                        price.Duration,
                        price.RemainingFixedPayments,
                        price.RemainingFloatPayments,
                        price.NextFloatFixingDate,
                        price.PricedAtUtc);
                },
                onError: ex =>
                    logger.LogError(ex, "[IRS 10Y] Permanently failed"),
                onCompleted: () =>
                    logger.LogInformation("[IRS 10Y] Stream completed normally"));

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stopping IRS subscriber - disposing clients");
        _client5Y?.Dispose();
        _client10Y?.Dispose();
        await base.StopAsync(cancellationToken);
    }
}
