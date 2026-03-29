using Streamly.Core.Abstractions;
using Streamly.Core.Models;

namespace Streamly.Server.Context;

internal interface IStreamingContextFactory<TRequest, in TResponse>
{
    /// <summary>
    /// Create a streaming context for a specific request.
    /// StreamBehavior determines auto-close behavior.
    /// </summary>
    IStreamingContext<TResponse> Create(
        string requestId,
        StreamBehavior streamBehavior);
}
