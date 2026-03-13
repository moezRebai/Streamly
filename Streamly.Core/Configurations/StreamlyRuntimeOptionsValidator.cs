using Microsoft.Extensions.Options;

namespace Streamly.Core.Configurations;

/// <summary>
/// Validates StreamlyRuntimeOptions configuration
/// </summary>
public class StreamlyRuntimeOptionsValidator : IValidateOptions<StreamlyRuntimeOptions>
{
    public ValidateOptionsResult Validate(string? name, StreamlyRuntimeOptions options)
    {
        // Validate ServiceName
        if (string.IsNullOrWhiteSpace(options.ServiceName))
            return ValidateOptionsResult.Fail(
                "ServiceName cannot be null or whitespace. " +
                "Provide a value in configuration under 'Streamly:ServiceName'.");

        if (options.ServiceName.Length > 50)
            return ValidateOptionsResult.Fail(
                $"ServiceName '{options.ServiceName}' is too long (max 50 characters).");

        // Validate InstanceId (computed or explicit)
        var instanceId = options.InstanceId;

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