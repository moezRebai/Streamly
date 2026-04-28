using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Streamly.Core.Abstractions;
using Streamly.Server.Leadership;

namespace Streamly.Server.Publishing;

internal sealed class ConfirmationQueue(
    string streamName,
    ConfirmationPublisher publisher,
    ILeaderElectionService leaderElection,
    IStreamlyMetricsCollector metrics,
    ILogger<ConfirmationQueue> logger)
    : IAsyncDisposable
{
    private readonly string _streamName = streamName ?? throw new ArgumentNullException(nameof(streamName));
    private readonly ConfirmationPublisher _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
    private readonly ILeaderElectionService _leaderElection = leaderElection ?? throw new ArgumentNullException(nameof(leaderElection));
    private readonly IStreamlyMetricsCollector _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
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

                const int maxAttempts = 3;
                Exception? lastEx = null;

                for (int attempt = 1; attempt <= maxAttempts; attempt++)
                {
                    try
                    {
                        await _publisher.ConfirmAsync(correlationId, requestId, cancellationToken);
                        lastEx = null;
                        break;
                    }
                    catch (Exception ex)
                    {
                        lastEx = ex;
                        _logger.LogWarning(ex,
                            "Confirmation attempt {Attempt}/{Max} failed for correlationId '{CorrelationId}' on stream '{StreamName}'",
                            attempt, maxAttempts, correlationId, _streamName);

                        if (attempt < maxAttempts)
                            await Task.Delay(200, cancellationToken);
                    }
                }

                if (lastEx is not null)
                {
                    _metrics.RecordConfirmationFailure(_streamName);
                    _logger.LogError(lastEx,
                        "All {Max} confirmation attempts exhausted for correlationId '{CorrelationId}' on stream '{StreamName}' — subscriber will reconnect after timeout",
                        maxAttempts, correlationId, _streamName);
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
