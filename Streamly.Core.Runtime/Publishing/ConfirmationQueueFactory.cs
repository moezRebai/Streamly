using Microsoft.Extensions.Logging;
using Streamly.Core.Runtime.Leadership;

namespace Streamly.Core.Runtime.Publishing;

/// <summary>
/// Factory for creating ConfirmationQueue instances per stream.
/// Uses ILeaderElectionFactory to get the correct per-stream leader election service,
/// same pattern as ConfirmationPublisherFactory.
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
