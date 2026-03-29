using Streamly.Client;
using Streamly.Client.Models;
using Streamly.Core.Models;

namespace Streamly.FakeSubscriber;

public class SwapSubscriberWorker(
    IStreamingSubscriber<IrsRequest, IrsResponse> subscriber,
    ILogger<SwapSubscriberWorker> logger)
    : BackgroundService
{
    private readonly List<IDisposable> _subscriptions = [];

    // 40 tenors × 50 notionals = 2000 combinations exactly
    private static readonly string[] Tenors =
    [
        "1M",  "2M",  "3M",  "6M",  "9M",  "18M",
        "1Y",  "2Y",  "3Y",  "4Y",  "5Y",  "6Y",  "7Y",  "8Y",  "9Y",  "10Y",
        "11Y", "12Y", "13Y", "14Y", "15Y", "16Y", "17Y", "18Y", "19Y", "20Y",
        "21Y", "22Y", "23Y", "24Y", "25Y", "26Y", "27Y", "28Y", "29Y", "30Y",
        "35Y", "40Y", "50Y", "60Y"
    ];

    private static readonly decimal[] Notionals =
    [
          500_000,   1_000_000,   1_500_000,   2_000_000,   2_500_000,
        3_000_000,   4_000_000,   5_000_000,   6_000_000,   7_000_000,
        8_000_000,   9_000_000,  10_000_000,  12_000_000,  15_000_000,
       17_500_000,  20_000_000,  22_500_000,  25_000_000,  27_500_000,
       30_000_000,  35_000_000,  40_000_000,  45_000_000,  50_000_000,
       55_000_000,  60_000_000,  65_000_000,  70_000_000,  75_000_000,
       80_000_000,  85_000_000,  90_000_000,  95_000_000, 100_000_000,
      120_000_000, 140_000_000, 160_000_000, 180_000_000, 200_000_000,
      225_000_000, 250_000_000, 275_000_000, 300_000_000, 350_000_000,
      400_000_000, 450_000_000, 500_000_000, 600_000_000, 750_000_000
    ];

    private static IEnumerable<IrsRequest> GenerateRequests()
    {
        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        return from tenor in Tenors
               from notional in Notionals
               select new IrsRequest
               {
                   Tenor         = tenor,
                   Notional      = notional,
                   EffectiveDate = today,
                   MaturityDate  = GetMaturityDate(today, tenor)
               };
    }

    private static DateOnly GetMaturityDate(DateOnly today, string tenor)
    {
        if (tenor.EndsWith('M'))
            return today.AddMonths(int.Parse(tenor.TrimEnd('M')));
        return today.AddYears(int.Parse(tenor.TrimEnd('Y')));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("SwapSubscriberWorker starting — subscribing to 2000 IRS streams");

        foreach (var request in GenerateRequests().Take(2000))
        {
            var label = $"{request.Tenor} {request.Notional / 1_000_000:N0}M";

            var sub = subscriber
                .Subscribe(
                    request,
                    behavior: StreamBehavior.Live,
                    onStatusChanged: status =>
                    {
                        if (status.State == StreamState.Reconnecting)
                            logger.LogWarning("[IRS] {Label} stream lost — {Message}", label, status.Message);
                        else if (status is { State: StreamState.Active, RetryAttempt: > 0 })
                            logger.LogInformation("[IRS] {Label} restored after {Attempts} attempt(s)", label, status.RetryAttempt);
                        else if (status.State == StreamState.Failed)
                            logger.LogError("[IRS] {Label} permanently failed — {Message}", label, status.Message);
                    })
                .Subscribe(
                    onNext: price =>
                        logger.LogTrace("[IRS] {Label} ParRate={ParRate:P4} NPV={Npv:N0} DV01={Dv01:F2}",
                            label, price.ParRate, price.Npv, price.Dv01),
                    onError: ex =>
                        logger.LogError(ex, "[IRS] {Label} error", label),
                    onCompleted: () =>
                        logger.LogDebug("[IRS] {Label} completed", label));

            _subscriptions.Add(sub);

            await Task.Delay(5, stoppingToken);
        }

        logger.LogInformation("SwapSubscriberWorker — all {Count} subscriptions active", _subscriptions.Count);
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("SwapSubscriberWorker stopping — disposing {Count} subscriptions", _subscriptions.Count);
        foreach (var sub in _subscriptions)
            sub.Dispose();
        _subscriptions.Clear();
        await base.StopAsync(cancellationToken);
    }
}
