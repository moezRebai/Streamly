// ═══════════════════════════════════════════════════════
// FILE: Streamly.Test.Publisher/Models.cs
// Simple request/response models for testing
// ═══════════════════════════════════════════════════════

namespace Streamly.FakeSubscriber;

public class SpotRequest
{
    public string CurrencyPair { get; set; } = string.Empty;
}

public class SpotPrice
{
    public string CurrencyPair { get; set; } = string.Empty;
    public decimal Bid { get; set; }
    public decimal Ask { get; set; }
    public DateTime Timestamp { get; set; }
}

public class IrsRequest
{
    public string   Tenor         { get; set; } = string.Empty; // e.g. 2Y, 5Y, 10Y, 30Y
    public decimal  Notional      { get; set; }                 // e.g. 10_000_000
    public DateOnly EffectiveDate { get; set; }
    public DateOnly MaturityDate  { get; set; }
}

// ─── Response ───────────────────────────────────────────────────────────────

public class IrsResponse
{
    // ── Swap identity (echoed from request) ─────────────────────────────────
    public string   Tenor         { get; set; } = string.Empty;
    public decimal  Notional      { get; set; }
    public DateOnly EffectiveDate { get; set; }
    public DateOnly MaturityDate  { get; set; }

    // ── Valuation — decimal (monetary precision) ─────────────────────────────
    public decimal  Npv           { get; set; }  // Net present value (USD)
    public decimal  ParRate       { get; set; }  // Mid-market par swap rate

    // ── Risk — double (sensitivity, not monetary) ─────────────────────────────
    public double   Dv01          { get; set; }  // Dollar value of 1bp (total)
    public double   Bpv           { get; set; }  // Basis point value
    public double   Duration      { get; set; }  // Modified duration (years)
    public double   Convexity     { get; set; }  // Second-order rate sensitivity
    public double   Cr01          { get; set; }  // Credit sensitivity (1bp spread)
    public double   Theta         { get; set; }  // Daily time decay

    // ── Notional in basis points — long ──────────────────────────────────────
    public long     NotionalBps   { get; set; }  // Notional × 10_000

    // ── Cashflow counts — int ─────────────────────────────────────────────────
    public int      RemainingFixedPayments  { get; set; }
    public int      RemainingFloatPayments  { get; set; }

    // ── Next fixing dates — DateTime ─────────────────────────────────────────
    public DateTime NextFixedPaymentDate    { get; set; }
    public DateTime NextFloatFixingDate     { get; set; }
    public DateTime NextFloatPaymentDate    { get; set; }

    // ── Metadata ─────────────────────────────────────────────────────────────
    public DateTime PricedAtUtc             { get; set; }

    public override string ToString()
    {
        return
            $"{nameof(Tenor)}: {Tenor}, {nameof(Notional)}: {Notional}, {nameof(EffectiveDate)}: {EffectiveDate}, {nameof(MaturityDate)}: {MaturityDate}, {nameof(Npv)}: {Npv}, {nameof(ParRate)}: {ParRate}, {nameof(Dv01)}: {Dv01}, {nameof(Bpv)}: {Bpv}, {nameof(Duration)}: {Duration}, {nameof(Convexity)}: {Convexity}, {nameof(Cr01)}: {Cr01}, {nameof(Theta)}: {Theta}, {nameof(NotionalBps)}: {NotionalBps}, {nameof(RemainingFixedPayments)}: {RemainingFixedPayments}, {nameof(RemainingFloatPayments)}: {RemainingFloatPayments}, {nameof(NextFixedPaymentDate)}: {NextFixedPaymentDate}, {nameof(NextFloatFixingDate)}: {NextFloatFixingDate}, {nameof(NextFloatPaymentDate)}: {NextFloatPaymentDate}, {nameof(PricedAtUtc)}: {PricedAtUtc}";
    }
}