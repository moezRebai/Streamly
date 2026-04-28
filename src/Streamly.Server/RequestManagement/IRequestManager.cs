using Streamly.Core.Models;

namespace Streamly.Server.RequestManagement;

/// <summary>
/// Non-generic lifecycle contract — allows <see cref="StreamlyHostedService"/> to start/stop
/// managers without reflection, regardless of their TRequest/TResponse type parameters.
/// </summary>
internal interface IRequestManager
{
    string StreamName { get; }
    Task StartAsync(CancellationToken cancellationToken = default);
    Task StopAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Full typed contract used by the rest of the server infrastructure.
/// </summary>
internal interface IRequestManager<TRequest, TResponse> : IRequestManager
{
    Task<string> OpenRequestAsync(RequestEnvelope<TRequest> envelope, CancellationToken cancellationToken = default);
    Task CloseRequestAsync(string requestId, CloseReason reason, CancellationToken cancellationToken = default);
    RequestMetadata<TRequest, TResponse>? GetRequest(string requestId);
    string[] GetActiveRequestIds();
    int ActiveRequestCount { get; }
}
