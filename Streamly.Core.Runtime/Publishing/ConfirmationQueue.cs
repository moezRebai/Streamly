// Decouples ConfirmationPublisher.ConfirmAsync from the NATS receive callback.
//
// Problem it solves
// ─────────────────
// OnRequestReceivedAsync runs inside a Task.Run dispatched by the NATS
// subscription loop. At burst scale (2k–10k simultaneous requests) thousands
// of those tasks all call ConfirmAsync concurrently, saturating NATS's internal
// CommandWriter and producing NatsTimeoutException.
//
// How it solves it
// ────────────────
// Callers enqueue (correlationId, requestId) and return immediately.
// A single background drain loop reads from the Channel and calls ConfirmAsync
// sequentially, at whatever pace NATS can sustain, with zero timeout pressure
// on the receive path.
//
// Lifecycle
// ─────────
// StartAsync  — starts the drain loop
// StopAsync   — completes the writer, waits for the drain loop to flush remaining items
// Owned by RequestManager, started/stopped alongside the other loops

using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Streamly.Core.Runtime.Leadership;

namespace Streamly.Core.Runtime.Publishing;

internal sealed class ConfirmationQueue(
    ConfirmationPublisher publisher,
    ILeaderElectionService leaderElection,
    ILogger<ConfirmationQueue> logger)
    : IAsyncDisposable
{
    private readonly ConfirmationPublisher _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
    private readonly ILeaderElectionService _leaderElection = leaderElection ?? throw new ArgumentNullException(nameof(leaderElection));
    private readonly ILogger<ConfirmationQueue> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    // Bounded at 32 768 — at 10k streams × ~100 bytes per entry ≈ 1 MB max.
    // SingleReader = true enables a faster lock-free read path.
    // FullMode = Wait provides back-pressure instead of silent drops.
    private readonly Channel<(string CorrelationId, string RequestId)> _channel =
        Channel.CreateBounded<(string, string)>(new BoundedChannelOptions(32_768)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

    private Task? _drainTask;
    private bool _disposed;

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    public void Start(CancellationToken cancellationToken)
    {
        _drainTask = DrainAsync(cancellationToken);
        _logger.LogDebug("ConfirmationQueue drain loop started");
    }

    public async Task StopAsync()
    {
        // Signal no more items will be written, then wait for the loop to flush.
        _channel.Writer.TryComplete();

        if (_drainTask is not null)
        {
            try
            {
                await _drainTask;
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellationToken is cancelled before queue drains
            }
        }

        _logger.LogDebug("ConfirmationQueue drain loop stopped");
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// <summary>
    /// Enqueues a confirmation to be sent asynchronously by the drain loop.
    /// Only call this when the current instance is the leader.
    /// Returns immediately — does not wait for the confirmation to be sent.
    /// </summary>
    public async ValueTask EnqueueAsync(
        string correlationId,
        string requestId,
        CancellationToken cancellationToken = default)
    {
        await _channel.Writer.WriteAsync(
            (correlationId, requestId),
            cancellationToken);
    }

    // ── Drain loop ────────────────────────────────────────────────────────────

    private async Task DrainAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var (correlationId, requestId) in
                _channel.Reader.ReadAllAsync(cancellationToken))
            {
                // Re-check leadership on every item — we may have lost it mid-drain.
                // The new leader will handle its own confirmations.
                if (!_leaderElection.IsLeader)
                {
                    _logger.LogTrace(
                        "Skipping confirmation for '{CorrelationId}' — no longer leader",
                        correlationId);
                    continue;
                }

                try
                {
                    await _publisher.ConfirmAsync(correlationId, requestId, cancellationToken);
                }
                catch (Exception ex)
                {
                    // Log and continue — subscriber will retry via Polly.
                    // Do not stop the loop; one failed confirmation must not
                    // block the thousands behind it.
                    _logger.LogError(ex,
                        "Failed to confirm correlationId '{CorrelationId}'", correlationId);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("ConfirmationQueue drain loop cancelled");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error in ConfirmationQueue drain loop");
        }
    }

    // ── Disposal ──────────────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await StopAsync();
    }
}
