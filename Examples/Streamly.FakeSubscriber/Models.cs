// ═══════════════════════════════════════════════════════
// FILE: Streamly.Test.Publisher/Models.cs
// Simple request/response models for testing
// ═══════════════════════════════════════════════════════

namespace Streamly.Subscriber;

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
