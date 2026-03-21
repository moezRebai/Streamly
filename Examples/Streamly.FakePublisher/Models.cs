// ═══════════════════════════════════════════════════════
// FILE: Streamly.Test.Publisher/Models.cs
// Simple request/response models for testing
// ═══════════════════════════════════════════════════════

using Streamly.Core.ChangeDetection;

namespace Streamly.FakePublisher;

public class SpotRequest
{
    public string CurrencyPair { get; set; } = string.Empty;
}

public class SpotPrice
{
    [IgnoreInComparison]
    public string CurrencyPair { get; set; } = string.Empty;
    public decimal Bid { get; set; }
    public decimal Ask { get; set; }
    
    [IgnoreInComparison]
    public DateTime Timestamp { get; set; }
}
