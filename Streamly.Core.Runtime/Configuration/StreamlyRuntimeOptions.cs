namespace Streamly.Core.Runtime.Configuration;

/// <summary>
/// Runtime configuration options
/// Bound from appsettings.json "Streamly" section
/// </summary>
public class StreamlyRuntimeOptions
{
    public const string SectionName = "Streamly";
    
    /// <summary>
    /// Unique identifier for this service instance
    /// Used for leader election and coordination
    /// Example: "PricingService-01", "PricingService-NY-Prod-02"
    /// </summary>
    public string InstanceId { get; set; } = GenerateDefaultInstanceId();
    
    /// <summary>
    /// Generate a default instance ID if none provided
    /// Format: MachineName-ProcessId
    /// </summary>
    private static string GenerateDefaultInstanceId()
    {
        return $"{Environment.MachineName}-{Environment.ProcessId}";
    }
}