using Streamly.Core.Models;

namespace Streamly.Server.Publishing.Interfaces;

internal interface IResponsePublisher<TRequest, in TResponse>
{
    Task PublishAsync(
        string requestId,
        TResponse response,
        CloseReason? closeReason = null,
        CancellationToken cancellationToken = default);

    Task CloseAsync(
        string requestId,
        CloseReason reason = CloseReason.Normal,
        CancellationToken cancellationToken = default);

    Task ForcePublishLatestImageAsync(
        string requestId,
        CancellationToken cancellationToken = default);
}
