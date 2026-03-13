using Microsoft.Extensions.Logging;
using Streamly.Core.Abstractions;
using Streamly.Core.Runtime.Leadership;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Core.Runtime.Publishing;

/// <summary>
/// Factory for creating ConfirmationPublisher instances per stream.
/// Uses ILeaderElectionFactory to get the correct per-stream leader election service.
/// 
/// </summary>
internal class ConfirmationPublisherFactory(
    ILeaderElectionFactory leaderElectionFactory,
    IStreamingTransport transport,
    IMessageSerializer serializer,
    ISubjectResolver subjects,
    ILoggerFactory loggerFactory)
    : IConfirmationPublisherFactory
{
    public async Task<ConfirmationPublisher> CreateAsync(string streamName)
    {
        // Get the per-stream leader election instance
        var leaderElection = await leaderElectionFactory.GetOrCreateAsync(streamName);

        return new ConfirmationPublisher(
            streamName,
            leaderElection,
            transport,
            serializer,
            subjects,
            loggerFactory.CreateLogger<ConfirmationPublisher>());
    }
}