using Streamly.Client;
using Streamly.Client.Models;
using Streamly.Core.Models;

namespace Streamly.FakeSubscriber;

public class SpotSubscriberWorker(
    IStreamingSubscriber<SpotRequest, SpotPrice> subscriber,
    ILogger<SpotSubscriberWorker> logger)
    : BackgroundService
{
    private readonly List<IDisposable> _subscriptions = [];

    // 50 × 50 = 2500 combinations; ~50 same-currency pairs filtered → 2450 valid → Take(2000)
    private static readonly string[] Bases = [
        "EUR", "GBP", "USD", "JPY", "CHF", "CAD", "AUD", "NZD", "NOK", "SEK",
        "DKK", "HKD", "SGD", "MXN", "ZAR", "TRY", "PLN", "CZK", "HUF", "ILS",
        "PHP", "IDR", "MYR", "THB", "KRW", "INR", "BRL", "CLP", "COP", "PEN",
        "ARS", "EGP", "NGN", "KES", "GHS", "SAR", "AED", "QAR", "KWD", "TWD",
        "VND", "UAH", "RON", "BGN", "RSD", "ISK", "LKR", "PKR", "BDT", "MAD"
    ];

    private static readonly string[] Quotes = [
        "EUR", "GBP", "USD", "JPY", "CHF", "CAD", "AUD", "NZD", "NOK", "SEK",
        "DKK", "HKD", "SGD", "MXN", "ZAR", "TRY", "PLN", "CZK", "HUF", "ILS",
        "PHP", "IDR", "MYR", "THB", "KRW", "INR", "BRL", "CLP", "COP", "PEN",
        "ARS", "EGP", "NGN", "KES", "GHS", "SAR", "AED", "QAR", "KWD", "TWD",
        "VND", "UAH", "RON", "BGN", "RSD", "ISK", "LKR", "PKR", "BDT", "MAD"
    ];

    private static IEnumerable<string> GeneratePairs() =>
        (from b in Bases
         from q in Quotes
         where b != q
         select $"{b}{q}")
        .Take(2000);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("SpotSubscriberWorker starting — subscribing to 2000 currency pairs");

        foreach (var pair in GeneratePairs())
        {
            var capturedPair = pair;

            Console.WriteLine(pair);
            var spotStream = subscriber
                .Subscribe(
                    new SpotRequest { CurrencyPair = capturedPair },
                    behavior: StreamBehavior.Live,
                    onStatusChanged: status =>
                    {
                        if (status.State == StreamState.Reconnecting)
                            logger.LogWarning("[Spot] {Pair} stream lost — {Message}", capturedPair, status.Message);
                        else if (status is { State: StreamState.Active, RetryAttempt: > 0 })
                            logger.LogInformation("[Spot] {Pair} restored after {Attempts} attempt(s)", capturedPair, status.RetryAttempt);
                        else if (status.State == StreamState.Failed)
                            logger.LogError("[Spot] {Pair} permanently failed — {Message}", capturedPair, status.Message);
                    });
            
            var disposable = spotStream
                .Subscribe(
                    onNext: price =>
                        logger.LogTrace("[Spot] {Pair} Bid={Bid:F5} Ask={Ask:F5}", price.CurrencyPair, price.Bid, price.Ask),
                    onError: ex =>
                        logger.LogError(ex, "[Spot] {Pair} error", capturedPair),
                    onCompleted: () =>
                        logger.LogDebug("[Spot] {Pair} completed", capturedPair));

            _subscriptions.Add(disposable);

            await Task.Delay(5, stoppingToken);
        }

        logger.LogInformation("SpotSubscriberWorker — all {Count} subscriptions active", _subscriptions.Count);
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("SpotSubscriberWorker stopping — disposing {Count} subscriptions", _subscriptions.Count);
        foreach (var sub in _subscriptions)
            sub.Dispose();
        _subscriptions.Clear();
        await base.StopAsync(cancellationToken);
    }
}
