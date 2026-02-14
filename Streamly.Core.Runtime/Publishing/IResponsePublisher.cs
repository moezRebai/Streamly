// File: Streamly.Core.Runtime/Publishing/IResponsePublisher.cs

using Streamly.Core.Models;

namespace Streamly.Core.Runtime.Publishing;

internal interface IResponsePublisher<TRequest, in TResponse>
{
    /// <summary>
    /// Publish response update
    /// closeReason: null = regular update
    /// closeReason: non-null = final update then close
    /// </summary>
    Task PublishAsync(
        string requestId,
        TResponse response,
        CloseReason? closeReason = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Close request without publishing response
    /// </summary>
    Task CloseAsync(
        string requestId,
        CloseReason reason = CloseReason.Normal,
        CancellationToken cancellationToken = default);
}