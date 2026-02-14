namespace Streamly.Core.Models;

/// <summary>
/// Event for cross-instance coordination
/// </summary>
public sealed class RequestEvent
{
    public required string RequestId { get; init; }
    public required RequestEventType Type { get; init; }
    public required long Epoch { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public int? SubscriberCount { get; init; }
    public RequestState? NewState { get; init; }
}