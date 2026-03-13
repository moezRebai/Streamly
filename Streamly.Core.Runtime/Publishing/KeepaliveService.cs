using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;
using Streamly.Core.Models;
using Streamly.Core.Runtime.Leadership;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Core.Runtime.Publishing;

/// <summary>
/// Publishes a lightweight heartbeat signal to subscribers
/// so they can distinguish "publisher alive but no data"
/// from "publisher dead".
/// 
/// Runs independently of pricing data — fires even during
/// market close, low volatility, or handler pauses.
/// </summary>
internal class KeepaliveService(
    string streamName,
    IStreamingTransport transport,
    ISubjectResolver subjects,
    IMessageSerializer serializer,
    ILeaderElectionService leaderElection,
    IOptions<StreamlyRuntimeOptions> options,
    ILogger<KeepaliveService> logger) : IAsyncDisposable
{
    private CancellationTokenSource? _cts;
    private Task? _loopTask;

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        _cts = new CancellationTokenSource();
        _loopTask = RunLoopAsync(_cts.Token);

        logger.LogInformation(
            "Keepalive service started for stream '{StreamName}' " +
            "(interval: {Interval}ms)",
            streamName, options.Value.PublisherHeartbeatIntervalMs);

        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        if (_cts == null) return;
        await _cts.CancelAsync();

        if (_loopTask != null)
        {
            try { await _loopTask; }
            catch (OperationCanceledException) { }
        }

        logger.LogInformation(
            "Keepalive service stopped for stream '{StreamName}'", streamName);
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        var subject = subjects.GetKeepaliveSubject(streamName);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(options.Value.PublisherHeartbeatIntervalMs, ct);

                // Only leader publishes keepalive
                // Followers are alive but not authoritative
                if (!leaderElection.IsLeader)
                    continue;

                var message = new KeepaliveMessage
                {
                    StreamName = streamName,
                    Timestamp = DateTime.UtcNow
                };

                var data = serializer.Serialize(message);
                await transport.PublishAsync(subject, data, ct);

                logger.LogTrace(
                    "Keepalive published for stream '{StreamName}'", streamName);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "Failed to publish keepalive for stream '{StreamName}'", streamName);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _cts?.Dispose();
    }
}