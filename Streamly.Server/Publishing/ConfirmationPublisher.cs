using Microsoft.Extensions.Logging;
using Streamly.Core.Abstractions;
using Streamly.Core.Models;
using Streamly.Server.Leadership;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Server.Publishing;

/// <summary>
/// Leader sends ConfirmationMessage back to subscriber after computing RequestId.
/// </summary>
internal class ConfirmationPublisher
{
    private readonly ILeaderElectionService _leaderElection;
    private readonly IStreamingTransport _transport;
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<ConfirmationPublisher> _logger;

    private readonly string _streamName;
    private readonly string _confirmSubject;

    public ConfirmationPublisher(
        string streamName,
        ILeaderElectionService leaderElection,
        IStreamingTransport transport,
        IMessageSerializer serializer,
        ISubjectResolver subjects,
        ILogger<ConfirmationPublisher> logger)
    {
        _streamName = streamName;
        _leaderElection = leaderElection ?? throw new ArgumentNullException(nameof(leaderElection));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _confirmSubject = subjects.GetConfirmSubject(_streamName);
    }

    public async Task ConfirmAsync(
        string correlationId,
        string requestId,
        CancellationToken cancellationToken = default)
    {
        if (!_leaderElection.IsLeader)
        {
            _logger.LogTrace(
                "Skipping confirmation for correlationId '{CorrelationId}' - not leader",
                correlationId);
            return;
        }

        try
        {
            var confirmation = new ConfirmationMessage
            {
                CorrelationId = correlationId,
                RequestId = requestId,
                StreamName = _streamName,
                Epoch = _leaderElection.CurrentEpoch,
                ConfirmedAt = DateTime.UtcNow
            };

            var data = _serializer.Serialize(confirmation);

            await _transport.PublishAsync(_confirmSubject, data, cancellationToken);

            _logger.LogInformation(
                "Sent confirmation: correlationId '{CorrelationId}' → requestId '{RequestId}'",
                correlationId,
                requestId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to send confirmation for correlationId '{CorrelationId}'",
                correlationId);
        }
    }
}
