using Microsoft.Extensions.Options;

namespace Streamly.Core.Runtime.Configuration;

/// <summary>
/// Validates StreamlyRuntimeOptions configuration
/// </summary>
public class StreamlyRuntimeOptionsValidator : IValidateOptions<StreamlyRuntimeOptions>
{
    public ValidateOptionsResult Validate(string? name, StreamlyRuntimeOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.InstanceId))
        {
            return ValidateOptionsResult.Fail(
                "InstanceId cannot be null or whitespace. " +
                "Provide a value in appsettings.json under 'Streamly:InstanceId' " +
                "or leave empty for auto-generation.");
        }
        
        // Validate no problematic characters for Redis keys
        if (options.InstanceId.Contains(':') || 
            options.InstanceId.Contains('*') || 
            options.InstanceId.Contains('?') ||
            options.InstanceId.Contains(' '))
        {
            return ValidateOptionsResult.Fail(
                $"InstanceId '{options.InstanceId}' contains invalid characters. " +
                "Avoid: colons (:), asterisks (*), question marks (?), and spaces.");
        }
        
        // Recommend reasonable length
        if (options.InstanceId.Length > 100)
        {
            return ValidateOptionsResult.Fail(
                $"InstanceId '{options.InstanceId}' is too long (max 100 characters).");
        }
        
        return ValidateOptionsResult.Success;
    }
}