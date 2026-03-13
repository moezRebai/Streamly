using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;
using Streamly.Core.Runtime.Leadership;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Core.Runtime.Publishing;

internal class KeepaliveServiceFactory(
    IStreamingTransport transport,
    ISubjectResolver subjects,
    IMessageSerializer serializer,
    ILeaderElectionFactory leaderElectionFactory,
    IOptions<StreamlyRuntimeOptions> options,
    ILogger<KeepaliveService> logger)
{
    public async Task<KeepaliveService> CreateAsync(
        string streamName,
        CancellationToken cancellationToken = default)
    {
        var leaderElection = await leaderElectionFactory.GetOrCreateAsync(
            streamName, cancellationToken);

        return new KeepaliveService(
            streamName,
            transport,
            subjects,
            serializer,
            leaderElection,
            options,
            logger);
    }
}