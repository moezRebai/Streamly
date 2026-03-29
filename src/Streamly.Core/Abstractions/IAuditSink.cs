using Streamly.Core.Models;

namespace Streamly.Core.Abstractions;

/// <summary>
/// Write-side audit abstraction. Called by the publisher runtime immediately
/// after a response has passed change detection and been published to NATS.
///
/// Contract:
/// - Must never block the publish path under any circumstances.
/// - Must never throw — all errors are swallowed and logged internally.
/// - If the underlying store is unavailable, records are silently dropped.
///   A price going out is always more important than its audit record.
///
/// The null implementation <see cref="NullAuditSink"/> is registered by
/// default in Streamly.Server. Call AddStreamlyAudit() to activate the
/// NATS JetStream implementation that writes to the STREAMLY_AUDIT stream.
/// </summary>
public interface IAuditSink
{
    /// <summary>
    /// Records a published price tick asynchronously.
    /// Implementations must enqueue the record and return immediately —
    /// the actual write to durable storage happens on a background drain loop.
    /// </summary>
    /// <param name="record">The audit record to store.</param>
    /// <param name="ct">
    /// Cancellation token. Implementations should respect cancellation on the
    /// enqueue step but must not propagate exceptions to the caller.
    /// </param>
    ValueTask RecordAsync(AuditRecord record, CancellationToken ct = default);
}

/// <summary>
/// No-op implementation of <see cref="IAuditSink"/>.
/// Registered as the default in Streamly.Server so that the publisher
/// compiles and runs correctly when AddStreamlyAudit() has not been called.
/// RecordAsync returns a completed ValueTask with zero allocation.
/// </summary>
public sealed class NullAuditSink : IAuditSink
{
    /// <summary>Shared singleton — no state, safe to reuse.</summary>
    public static readonly NullAuditSink Instance = new();

    /// <inheritdoc/>
    public ValueTask RecordAsync(AuditRecord record, CancellationToken ct = default)
        => ValueTask.CompletedTask;
}