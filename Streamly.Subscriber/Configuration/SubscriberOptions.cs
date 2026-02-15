namespace Streamly.Subscriber.Configuration;

/// <summary>
/// Configuration for the subscriber side
/// </summary>
public class SubscriberOptions
{
    public const string SectionName = "Streamly:Subscriber";

    /// <summary>
    /// Timeout waiting for leader confirmation after subscribing
    /// If no confirmation received within this time, retry
    /// Default: 2 seconds
    /// </summary>
    public TimeSpan ConfirmationTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Number of parallel dispatch workers per stream type
    /// Higher = more parallelism but more CPU usage
    /// Default: processor count
    /// </summary>
    public int DispatchWorkerCount { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Capacity of internal dispatch channel (backpressure buffer)
    /// If buffer full, oldest messages are dropped
    /// Default: 10,000
    /// </summary>
    public int DispatchChannelCapacity { get; set; } = 10_000;

    /// <summary>
    /// Reconnect policy: max retry attempts before giving up
    /// Default: 10 (matches architecture doc)
    /// </summary>
    public int MaxReconnectAttempts { get; set; } = 10;

    /// <summary>
    /// Initial delay before first reconnect attempt
    /// Default: 1 second
    /// </summary>
    public TimeSpan ReconnectInitialDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Maximum delay between reconnect attempts (exponential backoff cap)
    /// Default: 30 seconds
    /// </summary>
    public TimeSpan ReconnectMaxDelay { get; set; } = TimeSpan.FromSeconds(30);
    
    
    /// <summary>
    /// How long to wait without a response before considering publisher dead.
    /// For trading: 500ms. General use: 2000ms.
    /// Default: 2000ms
    /// </summary>
    public TimeSpan HeartbeatTimeout { get; set; } = TimeSpan.FromMilliseconds(2000);
}


/// <summary>
/// Thrown by the watchdog when no responses received within HeartbeatTimeout.
/// Caught by Polly in StreamingSubscriber to trigger reconnection.
/// </summary>
public class PublisherUnavailableException : Exception
{
    public PublisherUnavailableException(string message) : base(message) { }
    public PublisherUnavailableException(string message, Exception inner) : base(message, inner) { }
}