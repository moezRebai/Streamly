// Simulates a live IRS mid-market pricer.
//
// Publish cadence  : 500ms (IRS repricing is heavier than spot)
// Market simulation: par rate follows a mean-reverting random walk (±2bp/tick)
//                    all risk measures derived consistently from that rate move
// Change detection : default reflection detector fires on every tick since
//                    rates move continuously — intentional for load testing

using Streamly.Core.Abstractions;
using Streamly.Core.Models;

namespace Streamly.FakePublisher.IRSwap;

public class IrsPricingHandler
    : IStreamingRequestHandler<IrsRequest, IrsResponse>
{
    private static readonly TimeSpan PublishInterval = TimeSpan.FromMilliseconds(250);

    public async Task OnRequestOpenedAsync(
        IrsRequest request,
        IStreamingContext<IrsResponse> context,
        CancellationToken cancellationToken)
    {
        var baseRate    = BaseParRate(request.Tenor);
        var currentRate = baseRate;
        var rng         = new Random();
        var tenorYears  = TenorToYears(request.Tenor);

        while (!cancellationToken.IsCancellationRequested)
        {
            // Mean-reverting random walk: ±2bp per tick, pulling toward base rate
            var move = (rng.NextDouble() - 0.5) * 0.0004;
            currentRate += move + 0.01 * (baseRate - currentRate);
            currentRate  = Math.Max(0.0001, currentRate);

            await context.PublishAsync(
                BuildPrice(request, currentRate, tenorYears),
                cancellationToken);

            await Task.Delay(PublishInterval, cancellationToken);
        }
    }

    public Task OnRequestClosingAsync(
        IrsRequest request,
        CloseReason reason,
        CancellationToken cancellationToken) => Task.CompletedTask;

    // ─── Price construction ──────────────────────────────────────────────────

    private static IrsResponse BuildPrice(
        IrsRequest request,
        double     parRate,
        double     tenorYears)
    {
        var notional = (double)request.Notional;
        var dv01     = notional * tenorYears * 0.0001 / 100.0;
        var npv      = (decimal)((parRate - 0.0450) * notional * tenorYears * 0.93);
        var now      = DateTime.UtcNow;

        // Remaining payment counts depend on tenor and a semi-annual/quarterly schedule
        var yearsLeft            = Math.Max(0, (request.MaturityDate.ToDateTime(TimeOnly.MinValue) - now).TotalDays / 365.0);
        var remainingFixed       = (int)Math.Ceiling(yearsLeft * 2);  // semi-annual
        var remainingFloat       = (int)Math.Ceiling(yearsLeft * 4);  // quarterly

        return new IrsResponse
        {
            // Identity
            Tenor         = request.Tenor,
            Notional      = request.Notional,
            EffectiveDate = request.EffectiveDate,
            MaturityDate  = request.MaturityDate,

            // Valuation
            Npv     = Math.Round(npv, 2),
            ParRate = (decimal)Math.Round(parRate, 6),

            // Risk
            Dv01      = Math.Round(dv01, 4),
            Bpv       = Math.Round(dv01, 4),
            Duration  = Math.Round(tenorYears * 0.92, 4),
            Convexity = Math.Round(tenorYears * tenorYears * 0.5 * 0.0001, 6),
            Cr01      = Math.Round(dv01 * 0.12, 4),
            Theta     = Math.Round(-dv01 * 0.003, 4),

            // Notional in basis points
            NotionalBps = (long)(request.Notional * 10_000m),

            // Cashflow counts
            RemainingFixedPayments = remainingFixed,
            RemainingFloatPayments = remainingFloat,

            // Next fixing dates
            NextFixedPaymentDate = now.AddMonths(6),
            NextFloatFixingDate  = now.AddMonths(3).AddDays(-2),  // T-2 fixing convention
            NextFloatPaymentDate = now.AddMonths(3),

            // Metadata
            PricedAtUtc = now,
        };
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static double BaseParRate(string tenor) => tenor switch
    {
        "1Y"  => 0.0510,
        "2Y"  => 0.0490,
        "3Y"  => 0.0475,
        "5Y"  => 0.0458,
        "7Y"  => 0.0451,
        "10Y" => 0.0445,
        "15Y" => 0.0442,
        "20Y" => 0.0440,
        "30Y" => 0.0435,
        _     => 0.0450,
    };

    private static double TenorToYears(string tenor) => tenor switch
    {
        "1Y"  => 1,
        "2Y"  => 2,
        "3Y"  => 3,
        "5Y"  => 5,
        "7Y"  => 7,
        "10Y" => 10,
        "15Y" => 15,
        "20Y" => 20,
        "30Y" => 30,
        _     => 5,
    };
}
