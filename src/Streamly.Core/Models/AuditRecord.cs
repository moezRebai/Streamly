namespace Streamly.Core.Models;

/// <summary>
/// An immutable record of a single price published by the leader.
/// Created by the publisher runtime immediately after a successful publish
/// and written to the STREAMLY_AUDIT JetStream stream by NatsAuditSink.
///
/// Design notes:
/// - Payload is the raw serialized bytes — identical to what was sent to
///   subscribers over NATS. No re-serialization cost, no schema drift.
/// - Sequence is assigned by JetStream on write and populated by
///   NatsAuditReader on read. It is 0 on records that have not yet been
///   persisted (e.g. in the sink's in-memory buffer).
/// - Timestamp uses nanosecond precision from the NATS message metadata
///   on read, and DateTimeOffset.UtcNow on write (before JetStream assigns
///   its own timestamp). The JetStream timestamp is authoritative for
///   ordering and is what QueryAsync filters against.
/// </summary>
public sealed record AuditRecord
{
    /// <summary>
    /// The unique identifier of the stream this record belongs to.
    /// Matches the requestId used throughout the Streamly pipeline.
    /// </summary>
    public required string RequestId { get; init; }

    /// <summary>
    /// The registered stream name, e.g. "FxSpot", "FxSwap", "IRS".
    /// Matches the name used during handler registration.
    /// Determines the NATS subject: audit.{StreamName}.
    /// </summary>
    public required string StreamName { get; init; }

    /// <summary>
    /// The UTC timestamp at which the price was published.
    /// On write: set by the publisher to DateTimeOffset.UtcNow.
    /// On read: replaced with the authoritative JetStream message timestamp
    /// which has nanosecond precision.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// The JetStream sequence number for this record within the
    /// STREAMLY_AUDIT stream. Monotonically increasing, never resets,
    /// no gaps within a subject. Set to 0 before the record is persisted.
    /// Populated by NatsAuditReader when records are read back.
    /// </summary>
    public ulong Sequence { get; init; }

    /// <summary>
    /// The size of the serialized payload in bytes.
    /// Useful for storage estimation and anomaly detection
    /// (an unusually large payload may indicate a serialization issue).
    /// </summary>
    public required int PayloadSize { get; init; }

    /// <summary>
    /// The raw serialized response payload — the exact bytes published to
    /// NATS subscribers. The encoding matches whatever IMessageSerializer
    /// is configured in Streamly.Infrastructure (default: System.Text.Json).
    /// Consumers deserialize using the same serializer as the live pipeline.
    /// </summary>
    public required byte[] Payload { get; init; }
}