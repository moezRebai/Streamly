using Streamly.Core.ChangeDetection;

namespace Streamly.FakePublisher.Models;

public class SpotRequest
{
    public string CurrencyPair { get; set; } = string.Empty;
}

public class SpotPrice
{
    [AlwaysPublish]
    public string CurrencyPair { get; set; } = string.Empty;
    public decimal Bid { get; set; }
    public decimal Ask { get; set; }
    public DateTime Timestamp { get; set; }
}
