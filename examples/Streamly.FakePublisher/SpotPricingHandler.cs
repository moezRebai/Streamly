//
// Simulates a live pricing feed with random ticks.
//
// Supports benchmark currency pairs of the form "EUR/USD_4999":
//   - Strip the "_N" suffix to resolve the base price
//   - Fall back to a synthetic 1.0000 mid if the root pair is unknown
//     so the handler never closes with Error during benchmarks

using Streamly.Core.Abstractions;
using Streamly.Core.Models;
using Streamly.FakePublisher.Models;

namespace Streamly.FakePublisher;

public class SpotPricingHandler(ILogger<SpotPricingHandler> logger)
    : IStreamingRequestHandler<SpotRequest, SpotPrice>
{
    private static readonly Dictionary<string, decimal> BasePrices = new()
    {
        { "EUR/USD", 1.0850m },
        { "EUR/GBP", 0.8520m },
        { "GBP/USD", 1.2740m },
        { "USD/JPY", 149.50m }
    };

    private const decimal FallbackBasePrice = 1.0000m;
    private const decimal Spread            = 0.0002m; // 2 pip spread

    public async Task OnRequestOpenedAsync(
        SpotRequest request,
        IStreamingContext<SpotPrice> context,
        CancellationToken cancellationToken)
    {
        logger.LogDebug(
            "Handler opened for {CurrencyPair} - starting price stream",
            request.CurrencyPair);

        var rootPair = StripSuffix(request.CurrencyPair);

        if (!BasePrices.TryGetValue(rootPair, out var basePrice))
        {
            logger.LogDebug(
                "Unknown root pair '{RootPair}' (from '{CurrencyPair}'), using fallback base price",
                rootPair, request.CurrencyPair);

            basePrice = FallbackBasePrice;
        }

        var random = new Random();

        // Jitter the first tick to spread 2000 streams across the first second
        //await Task.Delay(random.Next(0, 1000), cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            basePrice += (decimal)(random.NextDouble() - 0.5) * 0.0003m;

            var price = BuildPrice(request.CurrencyPair, basePrice, random);
            await context.PublishAsync(price, cancellationToken);

            await Task.Delay(1 + random.Next(0, 20), cancellationToken);
        }
    }

    public Task OnRequestClosingAsync(
        SpotRequest request,
        CloseReason reason,
        CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Handler closing for {CurrencyPair}, reason: {Reason}",
            request.CurrencyPair, reason);

        return Task.CompletedTask;
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static string StripSuffix(string currencyPair)
    {
        var idx = currencyPair.LastIndexOf('_');
        return idx >= 0 ? currencyPair[..idx] : currencyPair;
    }

    private static SpotPrice BuildPrice(string pair, decimal mid, Random random)
    {
        var noise = (decimal)(random.NextDouble() - 0.5) * 0.00001m;

        return new SpotPrice
        {
            CurrencyPair = pair,
            Bid          = Math.Round(mid - Spread / 2 + noise, 5),
            Ask          = Math.Round(mid + Spread / 2 + noise, 5),
            Timestamp    = DateTime.UtcNow
        };
    }
}