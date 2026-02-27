using Microsoft.Extensions.Logging;
using Streamly.Core.Abstractions;
using Streamly.Core.Models;
using Streamly.Core.Runtime.Leadership;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Core.Runtime.Publishing;

/// <summary>
/// Leader sends ConfirmationMessage back to subscriber after computing RequestId
///
/// Flow:
///   1. All instances receive RequestEnvelope from streams.requests.{stream}
///   2. ALL instances compute RequestId (SHA256) and open request locally
///   3. ONLY LEADER publishes ConfirmationMessage to streams.confirm.{stream}
///   4. Subscriber receives confirmation, now knows real RequestId
///   5. Subscriber starts filtering streams.responses.{stream} by RequestId
///
/// Why only leader publishes confirmation:
///   - Prevents duplicate confirmations (all instances would send same thing)
///   - Leader is authoritative for subscriber count management
///   - Consistent with leader-only response publishing
///   
/// MIGRATION NOTE: Replaced IRedisConnectionManager → IStreamingTransport
///                 Replaced IChannelNameResolver → ISubjectResolver
///                 Need to add GetConfirmSubject() to ISubjectResolver!
/// </summary>
internal class ConfirmationPublisher
{
    private readonly ILeaderElectionService _leaderElection;
    private readonly IStreamingTransport _transport;
    private readonly IMessageSerializer _serializer;
    private readonly ISubjectResolver _subjects;
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
        _subjects = subjects ?? throw new ArgumentNullException(nameof(subjects));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _confirmSubject = _subjects.GetConfirmSubject(_streamName);
    }

    /// <summary>
    /// Send confirmation back to subscriber (leader only)
    /// Called after RequestId has been computed and request opened in registry
    /// </summary>
    public async Task ConfirmAsync(
        string correlationId,
        string requestId,
        CancellationToken cancellationToken = default)
    {
        // Only leader confirms
        // Followers process request locally but don't send confirmation
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

            // Don't throw - subscriber will timeout and retry via Polly
        }
    }
}