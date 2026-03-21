using Microsoft.Extensions.Logging;
using Streamly.Core.Abstractions;
using Streamly.Server.Leadership;
using Streamly.Infrastructure.Interfaces;
using Streamly.Server.Publishing.Interfaces;

namespace Streamly.Server.Publishing;

/// <summary>
/// Factory for creating ConfirmationPublisher instances per stream.
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
