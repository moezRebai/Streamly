namespace Streamly.Core.Runtime.Channel;

/// <summary>
/// Implementation of channel name resolver
/// All channel names follow consistent pattern: streams.{type}.{streamName}
/// </summary>
public class ChannelNameResolver : IChannelNameResolver
{
    private const string Prefix = "streams";

    public string GetRequestsChannel(string streamName)
    {
        Validate(streamName);
        return $"{Prefix}.requests.{streamName}";
    }

    public string GetConfirmChannel(string streamName)
    {
        Validate(streamName);
        return $"{Prefix}.confirm.{streamName}";
    }

    public string GetResponsesChannel(string streamName)
    {
        Validate(streamName);
        return $"{Prefix}.responses.{streamName}";
    }

    public string GetUnsubscribeChannel(string streamName)
    {
        Validate(streamName);
        return $"{Prefix}.unsubscribe.{streamName}";
    }

    public string GetHeartbeatChannel(string streamName)
    {
        Validate(streamName);
        return $"{Prefix}.heartbeat.{streamName}";
    }

    public string GetEventsChannel(string streamName)
    {
        Validate(streamName);
        return $"{Prefix}.events.{streamName}";
    }

    public string GetBatchChannel(string streamName)
    {
        Validate(streamName);
        return $"{Prefix}.batch.{streamName}";
    }

    public string GetLeaderLockKey(string streamName)
    {
        Validate(streamName);
        return $"streamly:leader:{streamName}";
    }

    private static void Validate(string streamName)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name cannot be null or whitespace", nameof(streamName));

        // Prevent invalid Redis channel characters
        if (streamName.Contains(':') ||
            streamName.Contains('*') ||
            streamName.Contains('?') ||
            streamName.Contains(' '))
        {
            throw new ArgumentException(
                $"Stream name '{streamName}' contains invalid characters. " +
                "Avoid: ':', '*', '?', ' '",
                nameof(streamName));
        }
    }
}
