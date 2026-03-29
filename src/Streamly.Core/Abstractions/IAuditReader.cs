using Streamly.Core.Models;

namespace Streamly.Core.Abstractions;

/// <summary>
/// Read-side audit abstraction. Provides two query patterns over the durable
/// audit record store:
///
/// 1. Time-range query — primary use case for incident investigation.
///    "What prices did we publish on FxSpot between 14:30 and 14:45?"
///
/// 2. Sequence replay — resume pattern for incremental consumers.
///    "Give me everything from sequence N onwards."
///
/// Both methods return <see cref="IAsyncEnumerable{T}"/> so the caller can
/// cancel mid-stream, which is important when a time range contains millions
/// of records and only the first anomaly is needed.
///
/// IAuditReader is registered by AddStreamlyAuditReader() — a separate
/// registration from AddStreamlyAudit() so that services which only need
/// to query (dashboard, support tools) do not pull in the write-side
/// infrastructure.
/// </summary>
public interface IAuditReader
{
    /// <summary>
    /// Queries audit records by time range, optionally filtered to a single
    /// request ID. The NATS server performs the time-range seek server-side —
    /// only records within the window are transferred over the network.
    /// Client-side filtering is applied for the optional requestId predicate.
    /// </summary>
    /// <param name="streamName">
    /// The registered stream name to query, e.g. "FxSpot", "FxSwap", "IRS".
    /// Maps to the NATS subject audit.{streamName}.
    /// </param>
    /// <param name="from">
    /// Start of the time window (inclusive). Records at or after this
    /// timestamp are returned.
    /// </param>
    /// <param name="to">
    /// End of the time window (exclusive). Records before this timestamp
    /// are returned.
    /// </param>
    /// <param name="requestId">
    /// Optional. When provided, only records matching this request ID are
    /// yielded. When null, all records in the time window are returned.
    /// </param>
    /// <param name="ct">
    /// Cancellation token. Cancelling stops the enumeration cleanly —
    /// the underlying NATS consumer is released immediately.
    /// </param>
    /// <returns>
    /// An async sequence of <see cref="AuditRecord"/> ordered by sequence
    /// number ascending (i.e. oldest first within the time window).
    /// </returns>
    IAsyncEnumerable<AuditRecord> QueryAsync(
        string streamName,
        DateTimeOffset from,
        DateTimeOffset to,
        string? requestId = null,
        CancellationToken ct = default);

    /// <summary>
    /// Replays audit records from a known sequence number onwards.
    /// Used by incremental consumers that checkpoint their position and
    /// resume without re-specifying a time range.
    /// </summary>
    /// <param name="streamName">
    /// The registered stream name to replay, e.g. "FxSpot", "FxSwap", "IRS".
    /// </param>
    /// <param name="fromSequence">
    /// The JetStream sequence number to start from (inclusive).
    /// Sequence numbers are globally ordered and never reset within a stream.
    /// </param>
    /// <param name="ct">
    /// Cancellation token. Cancelling stops the enumeration cleanly.
    /// </param>
    /// <returns>
    /// An async sequence of <see cref="AuditRecord"/> starting at
    /// <paramref name="fromSequence"/>, ordered ascending.
    /// </returns>
    IAsyncEnumerable<AuditRecord> ReplayFromAsync(
        string streamName,
        ulong fromSequence,
        CancellationToken ct = default);
}