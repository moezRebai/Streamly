// ═══════════════════════════════════════════════════════
// FILE: Streamly.Test.Publisher/SpotPricingHandler.cs
// Simulates a live pricing feed with random ticks.
//
// Supports benchmark currency pairs of the form "EUR/USD_4999":
//   - Strip the "_N" suffix to resolve the base price
//   - Fall back to a synthetic 1.0000 mid if the root pair is unknown
//     so the handler never closes with Error during benchmarks
// ═══════════════════════════════════════════════════════

using Streamly.Core.Abstractions;
using Streamly.Core.Models;

namespace Streamly.Publisher;

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
    private const decimal Spread = 0.0002m; // 2 pip spread

    public async Task OnRequestOpenedAsync(
        SpotRequest request,
        IStreamingContext<SpotPrice> context,
        CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Handler opened for {CurrencyPair} - starting price stream",
            request.CurrencyPair);

        // Strip benchmark index suffix: "EUR/USD_4999" → "EUR/USD"
        var rootPair = StripSuffix(request.CurrencyPair);

        if (!BasePrices.TryGetValue(rootPair, out var basePrice))
        {
            // Unknown root pair — use synthetic price so the handler stays open.
            // This is the normal path for benchmark pairs beyond the base set.
            logger.LogDebug(
                "Unknown root pair '{RootPair}' (from '{CurrencyPair}'), using fallback base price",
                rootPair,
                request.CurrencyPair);

            basePrice = FallbackBasePrice;
        }

        var random = new Random();

        // Publish initial price immediately — this is what the benchmark waits for.
        var initialPrice = BuildPrice(request.CurrencyPair, basePrice, random);
        await context.PublishAsync(initialPrice, cancellationToken: cancellationToken);

        logger.LogDebug(
            "Published initial price for {CurrencyPair}: Bid={Bid} Ask={Ask}",
            request.CurrencyPair,
            initialPrice.Bid,
            initialPrice.Ask);

        // Continuous ticking
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(300, cancellationToken);

            basePrice += (decimal)(random.NextDouble() - 0.5) * 0.0003m;

            var price = BuildPrice(request.CurrencyPair, basePrice, random);
            await context.PublishAsync(price, cancellationToken: cancellationToken);

            logger.LogDebug(
                "Tick for {CurrencyPair}: Bid={Bid} Ask={Ask}",
                request.CurrencyPair,
                price.Bid,
                price.Ask);
        }
    }

    public Task OnRequestClosingAsync(
        SpotRequest request,
        CloseReason reason,
        CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Handler closing for {CurrencyPair}, reason: {Reason}",
            request.CurrencyPair,
            reason);

        return Task.CompletedTask;
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Strips a benchmark index suffix from a currency pair name.
    /// "EUR/USD_4999" → "EUR/USD"
    /// "EUR/USD"      → "EUR/USD"  (no-op when no suffix present)
    /// </summary>
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