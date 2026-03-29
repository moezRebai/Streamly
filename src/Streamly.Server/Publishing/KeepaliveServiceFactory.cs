using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Server.Publishing;

internal class KeepaliveServiceFactory(
    IStreamingTransport transport,
    ISubjectResolver subjects,
    IMessageSerializer serializer,
    IOptions<StreamlySettings> options,
    ILogger<KeepaliveService> logger)
{
    public Task<KeepaliveService> CreateAsync(
        string streamName)
    {
        return Task.FromResult(new KeepaliveService(
            streamName,
            transport,
            subjects,
            serializer,
            options,
            logger));
    }
}
