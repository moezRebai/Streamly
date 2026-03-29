using System.Text.Json.Serialization;
using Streamly.Core.Models;

namespace Streamly.Server.Publishing;

/// <summary>
/// Internal message wrapping a response.
/// Contains epoch for split-brain protection.
/// NOT exposed to users - epoch stripped before delivery.
/// </summary>
internal class InternalResponseMessage<TResponse>
{
    [JsonPropertyName("requestId")]
    public string RequestId { get; set; } = string.Empty;

    [JsonPropertyName("data")]
    public TResponse Data { get; set; } = default!;

    [JsonPropertyName("epoch")]
    public long Epoch { get; set; }

    [JsonPropertyName("publisherId")]
    public string PublisherId { get; set; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }

    /// <summary>
    /// Names of properties that are meaningful in <see cref="Data"/>.
    /// Null = full snapshot (first message or close) — subscriber sets LocalImage directly.
    /// Non-null = delta — subscriber merges only these properties onto its LocalImage.
    /// </summary>
    [JsonPropertyName("changedProperties")]
    public HashSet<string>? ChangedProperties { get; set; }

    [JsonPropertyName("isFinal")]
    public bool IsFinal { get; set; }

    [JsonPropertyName("closeReason")]
    public CloseReason? CloseReason { get; set; }
}
