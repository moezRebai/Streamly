using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Streamly.Server.Leadership;

namespace Streamly.Server.Publishing;

internal sealed class ConfirmationQueue(
    ConfirmationPublisher publisher,
    ILeaderElectionService leaderElection,
    ILogger<ConfirmationQueue> logger)
    : IAsyncDisposable
{
    private readonly ConfirmationPublisher _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
    private readonly ILeaderElectionService _leaderElection = leaderElection ?? throw new ArgumentNullException(nameof(leaderElection));
    private readonly ILogger<ConfirmationQueue> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly Channel<(string CorrelationId, string RequestId)> _channel =
        Channel.CreateBounded<(string, string)>(new BoundedChannelOptions(32_768)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

    private Task? _drainTask;
    private bool _disposed;

    public void Start(CancellationToken cancellationToken)
    {
        _drainTask = DrainAsync(cancellationToken);
        _logger.LogDebug("ConfirmationQueue drain loop started");
    }

    public async Task StopAsync()
    {
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

    public async ValueTask EnqueueAsync(
        string correlationId,
        string requestId,
        CancellationToken cancellationToken = default)
    {
        await _channel.Writer.WriteAsync(
            (correlationId, requestId),
            cancellationToken);
    }

    private async Task DrainAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var (correlationId, requestId) in
                _channel.Reader.ReadAllAsync(cancellationToken))
            {
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

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await StopAsync();
    }
}
