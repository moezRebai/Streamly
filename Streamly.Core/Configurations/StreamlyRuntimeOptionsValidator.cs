using Microsoft.Extensions.Options;

namespace Streamly.Core.Configurations;

/// <summary>
/// Validates StreamlyOptions configuration
/// </summary>
public class StreamlyOptionsValidator : IValidateOptions<StreamlySettings>
{
    public ValidateOptionsResult Validate(string? name, StreamlySettings settings)
    {
        // Validate ServiceName
        if (string.IsNullOrWhiteSpace(settings.ServiceName))
            return ValidateOptionsResult.Fail(
                "ServiceName cannot be null or whitespace. " +
                "Provide a value in configuration under 'Streamly:ServiceName'.");

        if (settings.ServiceName.Length > 50)
            return ValidateOptionsResult.Fail(
                $"ServiceName '{settings.ServiceName}' is too long (max 50 characters).");

        // Validate InstanceId (computed or explicit)
        var instanceId = settings.InstanceId;

        if (string.IsNullOrWhiteSpace(instanceId))
            return ValidateOptionsResult.Fail(
                "InstanceId cannot be null or whitespace. " +
                "Provide a value in configuration under 'Streamly:InstanceId'.");

        if (instanceId.Contains(':') ||
            instanceId.Contains('*') ||
            instanceId.Contains('?') ||
            instanceId.Contains(' '))
            return ValidateOptionsResult.Fail(
                $"InstanceId '{instanceId}' contains invalid characters. " +
                "Avoid: colons (:), asterisks (*), question marks (?), and spaces.");

        if (instanceId.Length > 100)
            return ValidateOptionsResult.Fail(
                $"InstanceId '{instanceId}' is too long (max 100 characters).");

        return ValidateOptionsResult.Success;
    }
}
