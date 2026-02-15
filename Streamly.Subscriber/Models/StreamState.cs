namespace Streamly.Subscriber.Models;

public enum StreamState
{
    Active,        // Receiving prices normally
    Reconnecting,  // Lost publisher, retrying
    Failed         // All retries exhausted
}