// ═══════════════════════════════════════════════════════
// FILE: Streamly.Test.Publisher/SpotPricingHandler.cs
// Simulates a live pricing feed with random ticks
// ═══════════════════════════════════════════════════════

using Streamly.Core.Abstractions;
using Streamly.Core.Models;

namespace Streamly.Publisher;

public class SpotPricingHandler(ILogger<SpotPricingHandler> logger) : IStreamingRequestHandler<SpotRequest, SpotPrice>
{
    // Simulate base prices per pair
    private static readonly Dictionary<string, decimal> BasePrices = new()
    {
        { "EUR/USD", 1.0850m },
        { "EUR/GBP", 0.8520m },
        { "GBP/USD", 1.2740m },
        { "USD/JPY", 149.50m }
    };

    public async Task OnRequestOpenedAsync(
        SpotRequest request,
        IStreamingContext<SpotPrice> context,
        CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Handler opened for {CurrencyPair} - starting price stream",
            request.CurrencyPair);

        if (!BasePrices.TryGetValue(request.CurrencyPair, out var basePrice))
        {
            logger.LogWarning("Unknown currency pair: {CurrencyPair}", request.CurrencyPair);
            await context.CloseAsync(CloseReason.Error, cancellationToken);
            return;
        }

        var random = new Random();
        const decimal spread = 0.0002m; // 2 pip spread

        // Publish initial price immediately
        var initialPrice = BuildPrice(request.CurrencyPair, basePrice, spread, random);
        await context.PublishAsync(initialPrice, cancellationToken: cancellationToken);

        logger.LogInformation(
            "Published initial price for {CurrencyPair}: Bid={Bid} Ask={Ask}",
            request.CurrencyPair,
            initialPrice.Bid,
            initialPrice.Ask);

        // Simulate continuous ticking every 500ms
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(500, cancellationToken);

            // Random walk: price drifts slightly each tick
            basePrice += (decimal)(random.NextDouble() - 0.5) * 0.0003m;

            var price = BuildPrice(request.CurrencyPair, basePrice, spread, random);
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

        // Nothing to unsubscribe - we used Task.Delay loop
        // In real scenario: unsubscribe from market data feed here
        return Task.CompletedTask;
    }

    private static SpotPrice BuildPrice(
        string pair,
        decimal mid,
        decimal spread,
        Random random)
    {
        // Add small random noise
        var noise = (decimal)(random.NextDouble() - 0.5) * 0.00001m;

        return new SpotPrice
        {
            CurrencyPair = pair,
            Bid = Math.Round(mid - spread / 2 + noise, 5),
            Ask = Math.Round(mid + spread / 2 + noise, 5),
            Timestamp = DateTime.UtcNow
        };
    }
}
