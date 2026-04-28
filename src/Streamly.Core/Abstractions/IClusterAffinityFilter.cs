namespace Streamly.Core.Abstractions;

/// <summary>
/// Determines whether this cluster should handle a given request.
///
/// Register a typed implementation via DI to restrict which requests this cluster processes.
/// If not registered, the cluster handles all requests (backward-compatible default).
///
/// Example — cluster that only handles EURUSD and USDJPY:
/// <code>
/// public class SpotPricingClusterFilter : IClusterAffinityFilter&lt;SpotPricingRequest&gt;
/// {
///     public bool Accepts(SpotPricingRequest request)
///         => request.Instrument is "EURUSD" or "USDJPY";
/// }
///
/// // Registration
/// services.AddSingleton&lt;IClusterAffinityFilter&lt;SpotPricingRequest&gt;, SpotPricingClusterFilter&gt;();
/// </code>
///
/// When Accepts returns false this cluster silently ignores the request.
/// If no cluster accepts the request the subscriber's watchdog fires and
/// StreamStatus transitions to Failed with reason NoProvider.
/// </summary>
public interface IClusterAffinityFilter<in TRequest>
{
    /// <summary>
    /// Returns true if this cluster should open and publish this request.
    /// </summary>
    bool Accepts(TRequest request);
}
