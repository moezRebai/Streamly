using Microsoft.Extensions.Logging;
using Streamly.Server.Leadership;
using Streamly.Server.Publishing.Interfaces;

namespace Streamly.Server.Publishing;

/// <summary>
/// Factory for creating ConfirmationQueue instances per stream.
/// </summary>
internal class ConfirmationQueueFactory(
    ILeaderElectionFactory leaderElectionFactory,
    IConfirmationPublisherFactory confirmationPublisherFactory,
    ILoggerFactory loggerFactory)
    : IConfirmationQueueFactory
{
    public async Task<ConfirmationQueue> CreateAsync(string streamName)
    {
        var leaderElection = await leaderElectionFactory.GetOrCreateAsync(streamName);
        var publisher = await confirmationPublisherFactory.CreateAsync(streamName);

        return new ConfirmationQueue(
            publisher,
            leaderElection,
            loggerFactory.CreateLogger<ConfirmationQueue>());
    }
}
