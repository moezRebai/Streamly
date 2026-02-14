using Streamly.Core.Models;

namespace Streamly.Subscriber.Internal;

/// <summary>
/// Internal response message (matches what publisher sends)
/// </summary>
internal class InternalResponseMessage<TResponse>
{
    [System.Text.Json.Serialization.JsonPropertyName("requestId")]
    public string RequestId { get; set; } = string.Empty;

    [System.Text.Json.Serialization.JsonPropertyName("data")]
    public TResponse Data { get; set; } = default!;

    [System.Text.Json.Serialization.JsonPropertyName("epoch")]
    public long Epoch { get; set; }

    [System.Text.Json.Serialization.JsonPropertyName("isFinal")]
    public bool IsFinal { get; set; }

    [System.Text.Json.Serialization.JsonPropertyName("closeReason")]
    public CloseReason? CloseReason { get; set; }
}