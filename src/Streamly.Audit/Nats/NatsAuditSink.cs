using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using Streamly.Core.Abstractions;
using Streamly.Core.Models;
using Streamly.Infrastructure.Nats;

namespace Streamly.Audit.Nats;

/// <summary>
/// NATS JetStream implementation of <see cref="IAuditSink"/>.
///
/// Architecture:
/// - <see cref="RecordAsync"/> enqueues the record into a bounded
///   <see cref="Channel{T}"/> and returns immediately — the publish hot path
///   is never blocked regardless of JetStream availability.
/// - A background drain loop (started as <see cref="IHostedService"/>) reads
///   from the channel and publishes to the NATS JetStream stream.
/// - If the channel is full (JetStream unavailable for an extended period),
///   new records are dropped and a warning is logged.
/// - If JetStream publish fails, the record is retried once after a short
///   delay, then dropped. The publish pipeline is not affected.
///
/// Subject convention: audit.{StreamName} (e.g. audit.FxSpot, audit.IRS).
/// The STREAMLY_AUDIT JetStream stream captures all subjects matching audit.>
/// </summary>
internal sealed class NatsAuditSink : IAuditSink, IHostedService, IAsyncDisposable
{
    private readonly NatsConnectionManager _connectionManager;
    private readonly AuditOptions _options;
    private readonly ILogger<NatsAuditSink> _logger;

    private readonly Channel<AuditRecord> _channel;
    private CancellationTokenSource? _drainCts;
    private Task? _drainTask;
    private NatsJSContext? _js;

    public NatsAuditSink(
        NatsConnectionManager connectionManager,
        AuditOptions options,
        ILogger<NatsAuditSink> logger)
    {
        _connectionManager = connectionManager
            ?? throw new ArgumentNullException(nameof(connectionManager));
        _options = options
            ?? throw new ArgumentNullException(nameof(options));
        _logger = logger
            ?? throw new ArgumentNullException(nameof(logger));

        _channel = Channel.CreateBounded<AuditRecord>(
            new BoundedChannelOptions(_options.SinkBufferCapacity)
            {
                FullMode     = BoundedChannelFullMode.DropOldest,
                SingleReader = true,   // only the drain loop reads
                SingleWriter = false   // multiple publisher threads write
            });
    }

    // ── IAuditSink ────────────────────────────────────────────────────────────

    /// <inheritdoc/>
    /// <remarks>
    /// Enqueues into a bounded Channel — completes synchronously in the
    /// common case (channel not full). Never throws. If the channel is
    /// full the record is silently dropped (DropOldest policy).
    /// </remarks>
    public ValueTask RecordAsync(AuditRecord record, CancellationToken ct = default)
    {
        if (!_channel.Writer.TryWrite(record))
        {
            // Channel full — JetStream is likely unavailable.
            // DropOldest handles the eviction; we just log the drop.
            _logger.LogWarning(
                "Audit channel full — dropping record for stream '{StreamName}' " +
                "requestId '{RequestId}'. Check JetStream availability.",
                record.StreamName, record.RequestId);
        }

        return ValueTask.CompletedTask;
    }

    // ── IHostedService — drain loop lifecycle ─────────────────────────────────

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _js       = new NatsJSContext(_connectionManager.Connection);
        _drainCts = new CancellationTokenSource();
        _drainTask = RunDrainLoopAsync(_drainCts.Token);

        _logger.LogInformation(
            "NatsAuditSink drain loop started (buffer: {Capacity} records)",
            _options.SinkBufferCapacity);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_drainCts is null) return;

        // Signal drain loop to stop accepting new work
        await _drainCts.CancelAsync();

        // Complete the channel writer so the drain loop exits its await
        _channel.Writer.TryComplete();

        // Wait for the drain loop to flush remaining records
        if (_drainTask is not null)
        {
            try
            {
                await _drainTask.WaitAsync(
                    TimeSpan.FromSeconds(5),
                    cancellationToken);
            }
            catch (TimeoutException)
            {
                _logger.LogWarning(
                    "NatsAuditSink drain loop did not complete within 5s during shutdown. " +
                    "Some audit records may be lost.");
            }
            catch (OperationCanceledException) { }
        }

        _logger.LogInformation("NatsAuditSink drain loop stopped");
    }

    // ── Drain loop ────────────────────────────────────────────────────────────

    private async Task RunDrainLoopAsync(CancellationToken ct)
    {
        await foreach (var record in _channel.Reader.ReadAllAsync(ct))
        {
            await PublishWithRetryAsync(record, ct);
        }
    }

    private async Task PublishWithRetryAsync(AuditRecord record, CancellationToken ct)
    {
        var subject = $"{_options.SubjectPrefix}.{record.StreamName}";

        // Attach requestId as a NATS header so NatsAuditReader can filter
        // without deserializing the payload on the read path.
        var headers = new NATS.Client.Core.NatsHeaders
        {
            ["X-Streamly-RequestId"] = record.RequestId,
            ["X-Streamly-StreamName"] = record.StreamName
        };

        try
        {
            await _js!.PublishAsync(
                subject: subject,
                data: record.Payload,
                headers: headers,
                cancellationToken: ct);

            _logger.LogTrace(
                "Audit record published to '{Subject}' " +
                "(requestId: '{RequestId}', size: {Size}B)",
                subject, record.RequestId, record.PayloadSize);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to publish audit record to '{Subject}' — retrying once",
                subject);

            try
            {
                await Task.Delay(500, ct);
                await _js!.PublishAsync(
                    subject: subject,
                    data: record.Payload,
                    headers: headers,
                    cancellationToken: ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception retryEx)
            {
                _logger.LogError(retryEx,
                    "Audit record for '{RequestId}' dropped after retry failure on '{Subject}'",
                    record.RequestId, subject);
            }
        }
    }

    // ── Disposal ──────────────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        _drainCts?.Dispose();

        if (_drainTask is not null)
        {
            try { await _drainTask; }
            catch (OperationCanceledException) { }
        }
    }
}
