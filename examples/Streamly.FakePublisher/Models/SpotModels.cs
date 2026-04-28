using Streamly.Core.ChangeDetection;

namespace Streamly.FakePublisher.Models;

public class SpotRequest
{
    public string CurrencyPair { get; set; } = string.Empty;
}

public class SpotPrice
{
    [AlwaysPublish]
    public string CurrencyPair { get; set; }
    
    [PublishThreshold(0.00001)] 
    public decimal Bid { get; set; }
    
    [PublishThreshold(0.00001)] 
    public decimal Ask { get; set; }
    public DateTime Timestamp { get; set; }
}
