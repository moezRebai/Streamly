namespace Streamly.Client.Configuration;

/// <summary>
/// Thrown by the watchdog when no responses received within HeartbeatTimeout.
/// Caught by Polly in StreamingSubscriber to trigger reconnection.
/// </summary>
public class PublisherUnavailableException : Exception
{
    public PublisherUnavailableException(string message) : base(message) { }
    public PublisherUnavailableException(string message, Exception inner) : base(message, inner) { }
}
