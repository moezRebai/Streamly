namespace Streamly.Core.Configurations;

/// <summary>
/// Redis channel name builder
/// </summary>
public static class ChannelNames
{
    public static string Requests(string serviceName) => $"{serviceName}.requests";
    public static string Responses(string serviceName) => $"{serviceName}.responses";
    public static string Heartbeat(string serviceName) => $"{serviceName}:heartbeat";
    public static string Events(string serviceName) => $"{serviceName}:events";
    public static string BatchSync(string serviceName) => $"{serviceName}:batch";
    public static string LeaderLock(string serviceName) => $"{serviceName}:leader:lock";
}