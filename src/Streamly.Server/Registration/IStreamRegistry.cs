namespace Streamly.Server.Registration;

/// <summary>
/// Registry mapping request types to their configured stream names.
/// </summary>
public interface IStreamRegistry
{
    string GetStreamName<TRequest>();
    bool TryGetStreamName<TRequest>(out string? streamName);
    bool IsRegistered<TRequest>();
}
