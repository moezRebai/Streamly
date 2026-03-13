// FILE: Streamly.Benchmarks/Models.cs
//
// Must match the models used by Streamly.Test.Publisher exactly,
// because serialization happens over NATS and field names must align.
//
// Latency is measured as wall-clock time from Subscribe() call to first
// onNext callback, not as a field stamped by the publisher. This works
// because the publisher sends the initial price immediately on open,
// so the round-trip time is: Subscribe → NATS request → handler open
// → PublishAsync → NATS response → onNext.

namespace Streamly.Benchmarks;

public class BenchSpotRequest
{
    public string CurrencyPair { get; set; } = string.Empty;
}

public class BenchSpotPrice
{
    public string CurrencyPair { get; set; } = string.Empty;
    public decimal Bid { get; set; }
    public decimal Ask { get; set; }
    public DateTime Timestamp { get; set; }
}