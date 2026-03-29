namespace Streamly.Monitoring.Models;

/// <summary>
/// Live record for a single stream, returned by:
///   GET /streamly/streams          → array of all active streams
///   GET /streamly/streams/{id}     → single record, 404 if not found
///
/// The LatestImageJson field is the raw JSON string of the last published
/// TResponse, stored as-is by InMemoryMetricsCollector. It is embedded
/// directly into the HTTP response body without re-serialization, so the
/// endpoint has zero allocation cost for the payload content.
/// </summary>
public sealed record StreamDetailRecord
{
    /// <summary>Unique request identifier.</summary>
    public required string RequestId { get; init; }

    public string? RequestJson { get; init; }
    
    /// <summary>
    /// Registered stream name, e.g. "FxSpot", "FxSwap", "IRS".
    /// Matches the name used during handler registration.
    /// </summary>
    public required string StreamName { get; init; }

    /// <summary>
    /// Current stream state as a string: "Streaming", "Closing", "Closed".
    /// </summary>
    public required string State { get; init; }

    /// <summary>Number of active subscriber leases on this stream.</summary>
    public required int SubscriberCount { get; init; }

    /// <summary>Total prices published on this stream since it opened.</summary>
    public required long PublishCount { get; init; }

    /// <summary>
    /// Total publishes suppressed by the change detector on this stream.
    /// </summary>
    public required long SkipCount { get; init; }

    /// <summary>UTC time this stream was opened.</summary>
    public required DateTimeOffset OpenedAt { get; init; }

    /// <summary>UTC time of the last successful publish on this stream.</summary>
    public DateTimeOffset LastPublishedAt { get; init; }

    /// <summary>
    /// Time-to-first-response in milliseconds: from when the subscriber sent the request
    /// to when the first full snapshot was received. Zero if no response yet.
    /// </summary>
    public double FirstResponseLatencyMs { get; init; }

    /// <summary>Last observed end-to-end latency in milliseconds (publisher stamp → subscriber receipt).</summary>
    public double LastLatencyMs { get; init; }

    /// <summary>Exponential moving average latency in milliseconds (α=0.1, ~10-sample window).</summary>
    public double AvgLatencyMs { get; init; }

    /// <summary>
    /// The full latest published response as a raw JSON string.
    /// Null if no price has been published yet on this stream.
    ///
    /// Stored as a plain string (not JsonElement) to avoid JsonDocument
    /// buffer lifetime issues. Written directly into the HTTP response
    /// body by StreamsEndpoint without re-serialization.
    /// </summary>
    public string? LatestImageJson { get; init; }
}
