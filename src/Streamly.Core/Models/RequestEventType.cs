namespace Streamly.Core.Models;

/// <summary>
/// Event types for request lifecycle coordination
/// </summary>
public enum RequestEventType
{
    Opened = 0,
    Closed = 1,
    SubscriberAdded = 2,
    SubscriberRemoved = 3,
    ImageUpdated = 4
}