using Microsoft.Extensions.Logging;
using Streamly.Core.Runtime.Channel;
using Streamly.Core.Runtime.Leadership;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Core.Runtime.Publishing;

/// <summary>
/// Factory for creating ConfirmationPublisher instances per stream.
/// Uses ILeaderElectionFactory to get the correct per-stream leader election service.
/// </summary>
internal class ConfirmationPublisherFactory(
    ILeaderElectionFactory leaderElectionFactory, // ← Not ILeaderElectionService
    IRedisConnectionManager redis,
    IMessageSerializer serializer,
    IChannelNameResolver channelResolver,
    ILoggerFactory loggerFactory)
    : IConfirmationPublisherFactory
{
    // ← Factory, not service

    public ConfirmationPublisher Create(string streamName)
    {
        // Get the per-stream leader election instance
        var leaderElection = leaderElectionFactory.GetOrCreate(streamName);

        return new ConfirmationPublisher(
            streamName,
            leaderElection,       // ← Correct per-stream instance
            redis,
            serializer,
            channelResolver,
            loggerFactory.CreateLogger<ConfirmationPublisher>());
    }
}