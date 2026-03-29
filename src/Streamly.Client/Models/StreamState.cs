namespace Streamly.Client.Models;

public enum StreamState
{
    Active,        // Receiving prices normally
    Reconnecting,  // Lost publisher, retrying
    Failed         // All retries exhausted
}
