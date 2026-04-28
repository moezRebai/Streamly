namespace Streamly.Core.ChangeDetection;

/// <summary>
/// Only treat a numeric property as changed when the absolute difference
/// between old and new value meets or exceeds the given threshold.
/// Applies to any property type convertible to double (decimal, double,
/// float, int, long, …).
///
/// Example — suppress sub-pip noise on a 5dp price feed:
///   [PublishThreshold(0.00001)]
///   public decimal Bid { get; set; }
///
/// A threshold of 0 (default) behaves identically to no attribute.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public class PublishThresholdAttribute(double threshold) : Attribute
{
    public double Threshold { get; } = threshold;
}