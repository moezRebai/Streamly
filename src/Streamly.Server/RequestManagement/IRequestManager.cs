using Streamly.Core.Models;

namespace Streamly.Server.RequestManagement;

internal interface IRequestManager<TRequest, TResponse>
{
    string StreamName { get; }
    Task<string> OpenRequestAsync(RequestEnvelope<TRequest> envelope, CancellationToken cancellationToken = default);
    Task CloseRequestAsync(string requestId, CloseReason reason, CancellationToken cancellationToken = default);
    RequestMetadata<TRequest, TResponse>? GetRequest(string requestId);
    string[] GetActiveRequestIds();
    int ActiveRequestCount { get; }
    Task StartAsync(CancellationToken cancellationToken = default);
    Task StopAsync(CancellationToken cancellationToken = default);
}
